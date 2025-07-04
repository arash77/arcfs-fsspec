import aiohttp
import asyncio
from itertools import chain

class GitLabClient:
    def __init__(self, base_url, token):
        self._base_url = base_url
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

    async def retrieve_root_level(self, per_page=100) -> list[dict]:
        """
        Retrieve and return a list of GitLab project entries with metadata:
        - name (flattened path)
        - id
        - original_path
        - readable_path
        """
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

        # Fetch the first page to get total_pages
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
        remaining_pages = await asyncio.gather(*tasks)

        projects = [
            {
                "name": project["path_with_namespace"].replace("/", "-"),
                "id": project["id"],
                "original_path": project["path_with_namespace"],
                "readable_path": project["name_with_namespace"],
            }
            for project in chain(first_page, *remaining_pages)
        ]

        return projects
    
    async def retrieve_project_level(self, path, id, per_page=100, page=1) -> list[dict]:
        """
        List the contents of a path in the GitLab repository using keyset pagination.
        """
        # Ensure self._session is set
        await self._set_session()
        if self._session is None:
            raise RuntimeError("aiohttp session was not initialized")

        api_url = f"{self._base_url}/api/v4/projects/{id}/repository/tree"
        params = {
            "per_page": per_page,
            "pagination": "keyset",
            "recursive": "True",
        }

        all_files = []
        next_page = True
        while next_page:
            async with self._session.get(api_url, params=params) as resp:
                if resp.status != 200:
                    raise Exception(f"GitLab API error: {resp.status}")
                page = await resp.json()
                all_files.extend(page)

                # Retrieve the next page token from the response headers
                next_page = resp.links.get("next", {}).get("url")
                if not next_page:
                    continue
                api_url = next_page  # Update the URL for the next request

        return all_files