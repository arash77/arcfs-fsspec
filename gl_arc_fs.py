import fsspec
import fsspec.asyn
import aiohttp
import asyncio
from urllib.parse import quote
from pathlib import Path
from itertools import chain
from dotenv import load_dotenv
import os


class GitLab_ARC_FileSystem(fsspec.asyn.AsyncFileSystem):
    """
    GitLab ARC File System
    """

    def __init__(self, base_url ,*args, **kwargs):
        super().__init__(*args, **kwargs)
        self._base_url = base_url
        self._session = None
        self._repo_lookup = {}
        self.token = kwargs.get("token", None)
        self.semaphore = asyncio.Semaphore(kwargs.get("semaphore", 25))
        self.per_page = kwargs.get("per_page", 100)
        self.use_cache = kwargs.get("use_cache", True)
        self._cache_type = "dir"

    
    async def _set_session(self):
        if self._session is None:
            headers = {}
            if self.token is not None:
                headers["PRIVATE-TOKEN"] = self.token
            self._session = aiohttp.ClientSession(headers=headers)
        return self._session
    
    async def _ls(self, path, detail=True, **kwargs):
        """
        List the contents of a path in the GitLab repository.
        Uses caching unless refresh=True or caching is disabled.
        """ 
        refresh = kwargs.get("refresh", False)

        # Normalize path to string
        path = (path or "").strip("/")

        # Fetch root-level or project-level if not cached
        if not refresh or path not in self.dircache:
            if path == "":
                await self._retrieve_root_level()
            else:
                await self._retrieve_project_level(path)

        listing = self.dircache.get(path, [])
        return listing if detail else [d["name"] for d in listing]
    
    async def _info(self, path, **kwargs):
        """
        Get information about a file or directory in the GitLab repository.
        """
        if path == "" and path not in self.dircache:
            await self._retrieve_root_level()
            return self.dircache[""]
        elif path == "":
            return self.dircache[""]
        
        if path not in self.dircache:
            # If the path is not in the cache, retrieve it.
            await self._retrieve_project_level(path)
        return self.dircache.get(path)


    async def _retrieve_project_level(self, path):
        """
        List the contents of a path in the GitLab repository using keyset pagination.
        """
        await self._set_session()
        # Ensure self._session is set
        if self._session is None:
            raise RuntimeError("aiohttp session was not initialized")
        id = self._repo_lookup.get(path, {}).get("id", None)
        if id is None:
            await self._retrieve_root_level()
            id = self._repo_lookup.get(path, {}).get("id", None)
        if id is None:
            # If the ID is still not found, raise an error.
            raise ValueError(f"Project ID for path '{path}' not found in repository lookup.")
        api_url = f"{self._base_url}/api/v4/projects/{id}/repository/tree"
        params = {
            "per_page": self.per_page,
            "pagination": "keyset",
            "recursive": "True"  
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

        for file in all_files:
            file_path = str(Path(path, file["path"]))
            # Add each directory in dircache
            if file["type"] == "tree":
                self.dircache[file_path] = []
            parent_path = str(Path(file_path).parent) 
            #if parent_path == ".":
            #    parent_path = file_path
            if parent_path not in self.dircache:
                # Initialize the parent directory in dircache if not already present
                self.dircache[parent_path] = []
            val = {"name": file["name"],
                   "type": "dir" if file["type"] == "tree" else "file",
                   "size": None}
            # Add the file to the parent directory's cache
            self.dircache[parent_path].append(val)
        
        print("teschd")
        return self.dircache.get(path, [])

    async def _retrieve_root_level(self, per_page=100, page=1):
        """
        """
        await self._set_session()
        # Ensure self._session is set
        if self._session is None:
            raise RuntimeError("aiohttp session was not initialized")
        
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
            # Ensure self._session is set
            await self._set_session()
            if self._session is None:
                raise RuntimeError("aiohttp session was not initialized")
            async with self._session.get(api_url, params={"per_page": per_page,
                                                          "page": page,
                                                          "simple": "True",
                                                          "order_by": "last_activity_at",
                                                          "sort": "desc"}) as r:
                return await r.json()

        tasks = [fetch_page(page) for page in range(2, total_pages + 1)]
        remaining_pages = await asyncio.gather(*tasks)

        if not self.dircache.get("", None):
            self.dircache[""] = []
        for project in chain(first_page, *remaining_pages):
            val = {"name": project.get("path_with_namespace").replace("/", "-"),
                   "type": "dir",
                   "size": None,
                   "is_project": True}

            self.dircache[""].append(val)
            repo_entry = {"id": project.get("id"),
                          "original_path": project.get("path_with_namespace"),
                          "readable_path": project.get("name_with_namespace")}
            self._repo_lookup[val["name"]] = repo_entry
        
        return self.dircache.get("", [])
    

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
        load_dotenv()
        token = os.getenv("GITLAB_TOKEN")
        base_url = "https://git.nfdi4plants.org"
        teschd = GitLab_ARC_FileSystem(base_url=base_url, token=token)
        #result = await teschd._retrieve_root_level(per_page=100)
        #res = await teschd._retrieve_project_level("usadellab-Barvista_ARC")
        res = await teschd._ls("usadellab-Barvista_ARC", detail=False)
        print(res)


    asyncio.run(main())