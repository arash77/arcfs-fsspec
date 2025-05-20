import fsspec
import fsspec.asyn
import aiohttp
import asyncio
from urllib.parse import quote
from pathlib import Path
from itertools import chain

class GitLab_ARC_FileSystem(fsspec.asyn.AsyncFileSystem):
    """
    GitLab ARC File System
    """

    def __init__(self, base_url ,*args, **kwargs):
        super().__init__(*args, **kwargs)
        self._base_url = base_url
        self._cache = {"": None}
        self._session = None
        self.token = kwargs.get("token", None)
        self.semaphore = asyncio.Semaphore(kwargs.get("semaphore", 25))
        self.per_page = kwargs.get("per_page", 100)
        self.use_cache = kwargs.get("use_cache", True)
    
    async def _set_session(self):
        if self._session is None:
            self._session = aiohttp.ClientSession(headers={
                "PRIVATE-TOKEN": self.token
            })
        return self._session
    
    async def _ls(self, path, **kwargs):
        """
        List the contents of a path in the GitLab repository.
        """
        if path == "":
            await self._retrieve_root_level()
            return self._cache[""]

        # Check if the path is already cached
        if path in self._cache:
            return self._cache[path]

        # If not cached, retrieve the project level
        await self._retrieve_project_level(path)
        return self._cache[path]


    async def _retrieve_project_level(self, path):
        """
        List the contents of a path in the GitLab repository using keyset pagination.
        """
        await self._set_session()
        path_pre = "/" + path
        encoded_path = quote(path, safe="")
        api_url = f"{self._base_url}/api/v4/projects/{encoded_path}/repository/tree"
        params = {
            "per_page": self.per_page,
            "pagination": "keyset",
            "recursive": "True"  # Set to True if you want to list recursively
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

        flat = {}
        tree = {}
        data = {}
        for file in all_files:
            prefixed_path = self._prefix_path(path, file["path"])

            val = {"name": file["name"],
                   "type": "file" if file["type"] == "blob" else "dir",
                   "size": None}
            tree = val

            # Check if the file is a direct subdirectory of the path
            if self._is_direct_subdir(path_pre, prefixed_path):
                flat = val

            data[prefixed_path] =  {"tree": tree, "flat": flat}
        
        if self.use_cache:
            self._cache[path] = data
        
        return data

    async def _retrieve_root_level(self, per_page=100, page=1):
        """
        """
        await self._set_session()

        # Fetch the first page of projects
        api_url = f"{self._base_url}/api/v4/projects"
        params = {"per_page": per_page, "page": page,
                  "simple": "True",
                  "order_by": "last_activity_at",
                  "sort": "desc"}
        async with self._session.get(api_url, params=params) as resp:
            if resp.status != 200:
                raise Exception(f"GitLab API error: {resp.status}")
            first_page = await resp.json()

            # Get total pages from headers 
            total_pages = int(resp.headers.get("X-Total-Pages", 1))

        # fetch remaining pages
        # NOTE: This could be factored out and used for root and project level,
        # with params as argument.
        async def fetch_page(page):
            async with self._session.get(api_url, params={"per_page": per_page,
                                                          "page": page,
                                                          "simple": "True",
                                                          "order_by": "last_activity_at",
                                                          "sort": "desc"}) as r:
                return await r.json()

        tasks = [fetch_page(page) for page in range(2, total_pages + 1)]
        remaining_pages = await asyncio.gather(*tasks)

        for project in chain(first_page, *remaining_pages):
            val = {"name": project.get("name"),
                   "type": "dir",
                   "size": None}

            self._cache[""].update({project["path_with_namespace"]: val})

    def _prefix_path(self, path_prefix, path):
        """
        Prefix the path with the base URL.
        """
        return "/" + path_prefix + "/" + path

    def _is_direct_subdir(self, parent: str, child: str) -> bool:
        parent_path = Path(parent).resolve()
        child_path = Path(child).resolve()
        try:
            child_path.relative_to(parent_path)
        except ValueError:
            return False
        val = parent_path != child_path and child_path.parent == parent_path
        return val


if __name__ == "__main__":
    async def main():
        token = ""
        base_url = "https://git.nfdi4plants.org"
        teschd = GitLab_ARC_FileSystem(base_url=base_url, token=token)
        result = await teschd._retrieve_root_level(per_page=100)
        res = await teschd._retrieve_project_level("frsommer/AraTa_RB11_Interactions")
        # print(result)

    asyncio.run(main())