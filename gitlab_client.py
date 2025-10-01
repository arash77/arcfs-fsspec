import aiohttp
import asyncio
from itertools import chain
from urllib.parse import quote


class GitLabClient:
    """Thin async GitLab API wrapper used by the filesystem."""

    def __init__(self, base_url, token):
        self._base_url = base_url.rstrip("/")
        self._session = None
        self.token = token

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def _set_session(self):
        if self._session is None:
            headers = {}
            if self.token is not None:
                headers["PRIVATE-TOKEN"] = self.token
            self._session = aiohttp.ClientSession(headers=headers)
        return self._session

    async def get_project_by_path(self, original_path: str) -> dict | None:
        session = await self._set_session()
        api_url = f"{self._base_url}/api/v4/projects/{quote(original_path, safe='')}"
        async with session.get(api_url) as resp:
            if resp.status == 404:
                return None
            if resp.status != 200:
                raise Exception(f"GitLab API error: {resp.status}")
            project = await resp.json()
            return {
                "id": project["id"],
                "original_path": project["path_with_namespace"],
                "readable_path": project["name_with_namespace"],
            }

    async def get_project_by_id(self, project_id: int) -> dict | None:
        session = await self._set_session()
        api_url = f"{self._base_url}/api/v4/projects/{project_id}"
        async with session.get(api_url) as resp:
            if resp.status == 404:
                return None
            if resp.status != 200:
                raise Exception(f"GitLab API error: {resp.status}")
            project = await resp.json()
            return {
                "id": project["id"],
                "original_path": project["path_with_namespace"],
                "readable_path": project["name_with_namespace"],
            }

    async def retrieve_root_level(self, per_page=100) -> list[dict]:
        session = await self._set_session()
        if session is None:
            raise RuntimeError("aiohttp session was not initialized")

        api_url = f"{self._base_url}/api/v4/projects"
        base_params = {
            "per_page": per_page,
            "simple": "True",
            "order_by": "last_activity_at",
            "sort": "desc",
        }

        async with session.get(api_url, params={**base_params, "page": 1}) as resp:
            if resp.status != 200:
                raise Exception(f"GitLab API error: {resp.status}")
            first_page = await resp.json()
            total_pages = int(resp.headers.get("X-Total-Pages", 1))

        async def fetch_page(page: int):
            params = {**base_params, "page": page}
            async with session.get(api_url, params=params) as r:
                if r.status != 200:
                    raise Exception(f"GitLab API error on page {page}: {r.status}")
                return await r.json()

        tasks = [fetch_page(page) for page in range(2, total_pages + 1)]
        remaining_pages = await asyncio.gather(*tasks) if total_pages > 1 else []

        projects = [
            {
                "name": project["path_with_namespace"],
                "id": project["id"],
                "original_path": project["path_with_namespace"],
                "readable_path": project["name_with_namespace"],
            }
            for project in chain(first_page, *remaining_pages)
        ]
        return projects

    async def retrieve_project_level(self, repo_id: int, per_page=100, subdir="") -> list[dict]:
        await self._set_session()
        if self._session is None:
            raise RuntimeError("aiohttp session was not initialized")

        base_url = f"{self._base_url}/api/v4/projects/{repo_id}/repository/tree"
        params = {
            "per_page": per_page,
            "pagination": "keyset",
            "path": subdir,
        }

        async with self._session.get(base_url, params={**params, "page": 1}) as resp:
            if resp.status != 200:
                raise Exception(f"GitLab API error: {resp.status}")
            first_page = await resp.json()
            total_pages = int(resp.headers.get("X-Total-Pages", 1))

        async def fetch_page(page):
            async with self._session.get(base_url, params={**params, "page": page}) as r:
                if r.status != 200:
                    raise Exception(f"GitLab API error on page {page}: {r.status}")
                return await r.json()

        more_pages = (
            await asyncio.gather(*[fetch_page(p) for p in range(2, total_pages + 1)])
            if total_pages > 1
            else []
        )
        return list(chain(first_page, *more_pages))

    async def commit_and_push_lfs(self, repo_id, branch, filepath, sha, size):
        pointer_content = (
            "version https://git-lfs.github.com/spec/v1\n"
            f"oid sha256:{sha}\n"
            f"size {size}\n"
        )

        commit_url = f"{self._base_url}/api/v4/projects/{repo_id}/repository/commits"
        headers = {"PRIVATE-TOKEN": self.token}
        payload = {
            "branch": branch,
            "commit_message": f"Add LFS file {filepath}",
            "actions": [
                {
                    "action": "create",
                    "file_path": filepath,
                    "content": pointer_content,
                    "encoding": "text",
                }
            ],
        }

        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.post(commit_url, json=payload) as resp:
                if resp.status >= 300:
                    raise RuntimeError(f"Failed to commit LFS pointer file: {resp.status}")
                return await resp.json()

    async def create_merge_request(self, repo_id, source_branch, target_branch, title):
        mr_url = f"{self._base_url}/api/v4/projects/{repo_id}/merge_requests"
        headers = {"PRIVATE-TOKEN": self.token}
        payload = {
            "source_branch": source_branch,
            "target_branch": target_branch,
            "title": title,
            "remove_source_branch": True,
        }

        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.post(mr_url, json=payload) as resp:
                if resp.status == 409:
                    return
                if resp.status >= 300:
                    raise RuntimeError(f"Failed to create merge request: {resp.status}")
                return await resp.json()