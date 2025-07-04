import fsspec
import fsspec.asyn
import aiohttp
import asyncio
from urllib.parse import quote
from pathlib import Path
from itertools import chain
from dotenv import load_dotenv
import os
from aiofiles import open as aio_open  # async file access
from gitlab_client import GitLabClient  
from utils import split_first, get_repo_name_from_path, split_base_url, calculate_sha256



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
        self.gitlab_client = GitLabClient(base_url=base_url, token=kwargs.get("token", None))

    async def close(self):
        await self.gitlab_client.close()
    
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    def __del__(self):
        if self._session and not self._session.closed:
            import warnings
            warnings.warn(
                "GitLabClient session was not properly closed. "
                "Call `await client.close()` or use `async with`.",
                ResourceWarning,
            )

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
                await self._update_root_dir()
            else:
                await self._update_project_dir(path)

        listing = self.dircache.get(path, [])
        return listing if detail else [d["name"] for d in listing]

    async def _info(self, path, **kwargs):
        """
        Get information about a file or directory in the GitLab repository.
        """
        if path == "" and path not in self.dircache:
            await self._update_root_dir()
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
            await self._update_project_dir(repo_path)
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
            await self._update_root_dir()
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

    async def _update_root_dir(self):
        """
        Update the root directory cache by retrieving the root level projects.
        Also updates the _repo_lookup dictionary with project metadata.
        This is called when the root directory is accessed and not cached.
        """
        projects = await self.gitlab_client.retrieve_root_level()

        if not self.dircache.get("", None):
            self.dircache[""] = []
        for entry in projects:
            # entry = {"name", "id", "original_path", ...}
            self.dircache[""].append({
                "name": entry["name"],
                "type": "dir",
                "size": None,
                "is_project": True,
            })
            self._repo_lookup[entry["name"]] = {
                "id": entry["id"],
                "original_path": entry["original_path"],
                "readable_path": entry["readable_path"]
            } 

    async def _update_project_dir(self, path):
        id = self._repo_lookup.get(path, {}).get("id", None)
        if id is None:
            await self._update_root_dir()
            id = self._repo_lookup.get(path, {}).get("id", None)
        if id is None:
            # If the ID is still not found, raise an error.
            raise ValueError(
                f"Project ID for path '{path}' not found in repository lookup."
            )
        files = await self.gitlab_client.retrieve_project_level(path, id, self.per_page)

        for file in files:
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
        oid = await calculate_sha256(local_path)
    
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
        res = await teschd._ls("", detail=False)
        res = await teschd._ls("usadellab-Barvista_ARC", detail=False)
        print(res)
        # print(res)
        #await teschd._get_file("usadellab-Barvista_ARC/README.md", "README.md")
        #print(await teschd._exists("usadellab-Barvista_ARC/README.md"))
        #print(await teschd._exists("usadellab-Barvista_ARC/README."))
        #print(await teschd._isdir("usadellab-Barvista_ARC"))
        #print(await teschd._isfile("usadellab-Barvista_ARC/README.md"))
        #print(await teschd._info("usadellab-Barvista_ARC/README.md"))

        #bala = teschd.ls("usadellab-Barvista_ARC", detail=True)
        await teschd.close()

    asyncio.run(main())
