from fsspec.asyn import sync_wrapper, AbstractAsyncStreamedFile
import aiofiles
from urllib.parse import quote
from utils import get_repo_name_from_path, split_first
import aiohttp
from hashlib import sha256
from datetime import datetime
import urllib.parse
import io


tempfile = aiofiles.tempfile


class AsyncLFSFile(AbstractAsyncStreamedFile):
    """Async streamed file with Git LFS-backed writes and raw reads."""

    def __init__(self, fs, path, token, host, namespace, repo_id, ref="main", mode="rb", **kwargs):
        super().__init__(fs=fs, path=path, mode=mode, autocommit=True, **kwargs)
        self.token = token
        self.host = host
        self.namespace = namespace
        self.repo_id = repo_id
        self.ref = ref
        self._tmp = None
        self.shasum = sha256()
        self.changed = False
        self.branch_name = f"upload/{datetime.now().strftime('%Y-%m-%dT%H-%M-%S')}"
        self._downloaded = False

    async def __aenter__(self):
        await self._ensure_tmp()
        if 'r' in self.mode and not self._downloaded:
            await self._download_from_gitlab()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self._commit()
        if self._tmp:
            await self._tmp.close()

    async def _ensure_tmp(self):
        if self._tmp is None:
            self._tmp = await tempfile.TemporaryFile(mode="w+b")

    async def _initiate_upload(self):
        await self._ensure_tmp()
        await self._tmp.seek(0)

    async def _upload_chunk(self, final: bool = False):
        await self._ensure_tmp()
        self.shasum.update(self.buffer.getvalue())
        await self._tmp.write(self.buffer.getvalue())
        self.buffer.seek(0)
        self.buffer.truncate(0)
        self.changed = True
        if final:
            await self._tmp.flush()
            await self._tmp.seek(0)

    async def _fetch_range(self, start: int, end: int) -> bytes:
        await self._ensure_tmp()
        await self._tmp.seek(start)
        return await self._tmp.read(end - start)

    async def read(self, size=-1):
        await self._ensure_tmp()
        if 'r' not in self.mode:
            raise IOError("File not open for reading")
        if not self._downloaded:
            await self._download_from_gitlab()
        return await self._tmp.read(size)

    async def seek(self, offset, whence=io.SEEK_SET):
        await self._ensure_tmp()
        return await self._tmp.seek(offset, whence)

    async def tell(self):
        await self._ensure_tmp()
        return await self._tmp.tell()

    async def _download_from_gitlab(self):
        api_url = f"{self.host}/api/v4/projects/{self.repo_id}/repository/files/{quote(self.path)}/raw"
        params = {"lfs": "True", "ref": self.ref}
        headers = {"PRIVATE-TOKEN": self.token}
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(api_url, params=params) as resp:
                if resp.status != 200:
                    raise FileNotFoundError(f"Failed to fetch {self.path}: {resp.status}")
                data = await resp.read()
                await self._tmp.write(data)
                await self._tmp.seek(0)
                self._downloaded = True

    async def _commit(self):
        if not self.changed:
            return

        await self._ensure_tmp()
        await self._tmp.seek(0, io.SEEK_END)
        size = await self._tmp.tell()
        await self._tmp.seek(0)
        sha = self.shasum.hexdigest()

        batch_url = f"{self.host}/{self.namespace}.git/info/lfs/objects/batch"
        headers = {
            "Accept": "application/vnd.git-lfs+json",
            "Content-Type": "application/vnd.git-lfs+json",
        }
        json_payload = {
            "operation": "upload",
            "objects": [{"oid": sha, "size": size}],
            "transfers": ["basic"],
            "ref": {"name": f"refs/heads/{self.branch_name}"},
        }
        auth = aiohttp.BasicAuth("oauth2", self.token)

        async with aiohttp.ClientSession(auth=auth) as session:
            async with session.post(batch_url, headers=headers, json=json_payload) as resp:
                batch_resp = await resp.json()

            try:
                upload_info = batch_resp["objects"][0]["actions"]["upload"]
                upload_url = upload_info["href"]
                upload_headers = upload_info["header"]
                upload_headers.pop("Transfer-Encoding", None)

                async with session.put(upload_url, headers=upload_headers, data=self._tmp) as upload_resp:
                    if upload_resp.status >= 400:
                        raise Exception(f"LFS upload failed: {upload_resp.status}")

                await self.fs.gitlab_client.commit_and_push_lfs(
                    repo_id=self.repo_id,
                    branch=self.branch_name,
                    filepath=self.path,
                    sha=sha,
                    size=size,
                )

                await self.fs.gitlab_client.create_merge_request(
                    repo_id=self.repo_id,
                    source_branch=self.branch_name,
                    target_branch=self.ref,
                    title=f"LFS upload {self.path}",
                )

            except KeyError:
                # If the server indicates the object already exists, actions.upload may be missing.
                pass