import fsspec
import fsspec.asyn
import aiohttp
import asyncio
from urllib.parse import quote
from pathlib import Path
from itertools import chain
from dotenv import load_dotenv
import os
import hashlib
from aiofiles import open as aio_open  # async file access
from utils import split_first, get_repo_name_from_path, split_base_url



class GitLab_ARC_FileSystem(fsspec.asyn.AsyncFileSystem):
    """
    GitLab ARC File System
    """

    def __init__(self, base_url, *args, **kwargs):
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

        repo_path, inside_path = map(str, split_first(path))

        # NOTE: Instead of retieving the project level directly, we could also
        # check first if the path is a repository toplevel. Thic could be done
        # by iterating trhough self.dircache[""] and checking if the path is in
        # there. Could potentially save some requests.
        if repo_path not in self.dircache:
            # If the path is not in the cache, retrieve it.
            await self._retrieve_project_level(repo_path)
        info = self.dircache.get(repo_path, None)
        if info is None:
            # If the path is still not found, raise an error.
            raise FileNotFoundError(f"Path '{path}' not found in repository.")
        if path == repo_path:
            # If the path is the repository root, return its info.
            return {
                "name": Path(repo_path).name,
                "type": "directory",
                "size": None,
                "path": str(Path(repo_path)),
                "is_project": True,
            }
        for item in info:
            if item["name"] == Path(inside_path).name:
                # Return the item if it matches the inside path.
                return {
                    "name": item["name"],
                    "type": item["type"],
                    "size": item.get("size", None),
                    "path": str(Path(repo_path, inside_path)),
                    "is_project": item.get("is_project", False),
                }
        # If no item matches, raise an error.
        raise FileNotFoundError(f"Path '{path}' not found in repository.")
    
    async def _put_file(self, lpath, rpath, mode="overwrite", **kwargs):
        return await super()._put_file(lpath, rpath, mode, **kwargs)

    async def _get_file(self, rpath, lpath, **kwargs):
        """ "
        Get the content of a file in a GitLab repository and save it locally.
        """
        # Get the repository name from the path, the id and the path of the file inside the repository.
        repo_name = get_repo_name_from_path(rpath)
        id = self._repo_lookup.get(repo_name, {}).get("id", None)
        _, inside_path = split_first(rpath)

        if id is None:
            await self._retrieve_root_level()
            id = self._repo_lookup.get(repo_name, {}).get("id", None)
        if id is None:
            # If the ID is still not found, raise an error.
            raise ValueError(
                f"Project ID for path '{rpath}' not found in repository lookup."
            )
        api_url = f"{self._base_url}/api/v4/projects/{id}/repository/files/{quote(str(inside_path))}/raw"
        await self._set_session()
        if self._session is None:
            raise RuntimeError("aiohttp session was not initialized")
        async with self._session.get(
            api_url, params={"lfs": "True", "ref": "main"}
        ) as r:
            if r.status != 200:
                raise Exception(f"Failed to download {rpath}: {r.status}")
            async with aio_open(lpath, "wb") as f:
                async for chunk in r.content.iter_chunked(2**20):  # 1 MB
                    await f.write(chunk)
        return

    async def _exists(self, path, **kwargs):
        """
        Check if a path exists in the GitLab repository.
        """
        try:
            info = await self._info(path, **kwargs)
            return info is not None
        except FileNotFoundError:
            return False

    async def _isdir(self, path):
        try:
            info = await self._info(path)
            if info is None:
                return False
            return info["type"] == "directory"
        except FileNotFoundError:
            return False

    async def _isfile(self, path):
        try:
            info = await self._info(path)
            if info is None:
                return False
            return info["type"] == "file"
        except FileNotFoundError:
            return False

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
            raise ValueError(
                f"Project ID for path '{path}' not found in repository lookup."
            )
        api_url = f"{self._base_url}/api/v4/projects/{id}/repository/tree"
        params = {
            "per_page": self.per_page,
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

        for file in all_files:
            file_path = str(Path(path, file["path"]))
            # Add each directory in dircache
            if file["type"] == "tree":
                self.dircache[file_path] = []
            parent_path = str(Path(file_path).parent)
            # if parent_path == ".":
            #    parent_path = file_path
            if parent_path not in self.dircache:
                # Initialize the parent directory in dircache if not already present
                self.dircache[parent_path] = []
            val = {
                "name": file["name"],
                "type": "dir" if file["type"] == "tree" else "file",
                "size": None,
            }
            # Add the file to the parent directory's cache
            self.dircache[parent_path].append(val)

        print("teschd")
        return self.dircache.get(path, [])

    async def _retrieve_root_level(self, per_page=100, page=1):
        """ """
        await self._set_session()
        # Ensure self._session is set
        if self._session is None:
            raise RuntimeError("aiohttp session was not initialized")

        # Fetch the first page of projects
        api_url = f"{self._base_url}/api/v4/projects"
        params = {
            "per_page": per_page,
            "page": page,
            "simple": "True",
            "order_by": "last_activity_at",
            "sort": "desc",
        }
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
            async with self._session.get(
                api_url,
                params={
                    "per_page": per_page,
                    "page": page,
                    "simple": "True",
                    "order_by": "last_activity_at",
                    "sort": "desc",
                },
            ) as r:
                return await r.json()

        tasks = [fetch_page(page) for page in range(2, total_pages + 1)]
        remaining_pages = await asyncio.gather(*tasks)

        if not self.dircache.get("", None):
            self.dircache[""] = []
        for project in chain(first_page, *remaining_pages):
            val = {
                "name": project.get("path_with_namespace").replace("/", "-"),
                "type": "dir",
                "size": None,
                "is_project": True,
            }

            self.dircache[""].append(val)
            repo_entry = {
                "id": project.get("id"),
                "original_path": project.get("path_with_namespace"),
                "readable_path": project.get("name_with_namespace"),
            }
            self._repo_lookup[val["name"]] = repo_entry

        return self.dircache.get("", [])

    async def _upload_lfs_file_async(self, local_path: str, file_size: int, ref: str, url: str) -> str:
        """
        Upload a file to GitLab via the LFS Batch API (async version).

        Args:
            local_path (str): Local file path to upload
            file_size (int): Size of the file in bytes
            ref (str): Target branch (e.g., "upload/2025-06-04T15-20-11")

        Returns:
            str: SHA256 object ID of the uploaded LFS file
        """
        await self._set_session()
        if self._session is None:
            raise RuntimeError("aiohttp session was not initialized")

        # Step 1: Compute SHA-256 OID
        sha256 = hashlib.sha256()
        async with aio_open(local_path, "rb") as f:
            while True:
                chunk = await f.read(8192)
                if not chunk:
                    break
                sha256.update(chunk)
        oid = sha256.hexdigest()

        # Step 2: Prepare LFS batch request
        lfs_payload = {
            "operation": "upload",
            "transfers": ["basic"],
            "ref": {"name": f"refs/heads/{ref}"},
            "hash_algo": "sha256",
            "objects": [
                {
                    "oid": oid,
                    "size": file_size
                }
            ]
        }

        headers = {
            "Accept": "application/vnd.git-lfs+json",
            "Content-Type": "application/vnd.git-lfs+json"
        }

        host, namespace = split_base_url(url)
        batch_url = (
            f"https://oauth2:{self.token}@{host}/"
            f"{namespace}.git/info/lfs/objects/batch"
        )

        # Step 3: Call LFS batch API
        async with self._session.post(batch_url, json=lfs_payload, headers=headers) as resp:
            if resp.status != 200:
                raise Exception(f"LFS batch API failed: {resp.status}")
            result = await resp.json()

        # Step 4: Extract upload instructions
        try:
            upload_info = result["objects"][0]["actions"]["upload"]
            upload_url = upload_info["href"]
            upload_headers = upload_info.get("header", {})
            upload_headers.pop("Transfer-Encoding", None)
        except KeyError:
            print("LFS object already exists or upload skipped.")
            return oid

        # Step 5: Upload binary data to pre-signed S3 URL
        async with aio_open(local_path, "rb") as f:
            async with self._session.put(
                upload_url, headers=upload_headers, data=f
            ) as upload_resp:
                if upload_resp.status >= 300:
                    raise Exception(f"LFS object upload failed: {upload_resp.status}")

        print(f"LFS upload successful: oid={oid}")
        return oid


if __name__ == "__main__":

    async def main():
        load_dotenv()
        token = os.getenv("GITLAB_TOKEN")
        base_url = "https://git.nfdi4plants.org"
        teschd = GitLab_ARC_FileSystem(base_url=base_url, token=token)
        # result = await teschd._retrieve_root_level(per_page=100)
        # res = await teschd._retrieve_project_level("usadellab-Barvista_ARC")
        # res = await teschd._ls("usadellab-Barvista_ARC", detail=False)
        # print(res)
        #await teschd._get_file("usadellab-Barvista_ARC/README.md", "README.md")
        #print(await teschd._exists("usadellab-Barvista_ARC/README.md"))
        #print(await teschd._exists("usadellab-Barvista_ARC/README."))
        #print(await teschd._isdir("usadellab-Barvista_ARC"))
        #print(await teschd._isfile("usadellab-Barvista_ARC/README.md"))
        #print(await teschd._info("usadellab-Barvista_ARC/README.md"))

        bala = teschd.ls("usadellab-Barvista_ARC", detail=True)
        print(bala)

    asyncio.run(main())
