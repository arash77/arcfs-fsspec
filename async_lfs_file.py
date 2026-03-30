"""async_lfs_file.py

Async streamed file used by GitLabARCFileSystem.
"""

from __future__ import annotations

import io
from hashlib import sha256

import aiofiles
from fsspec.asyn import AbstractAsyncStreamedFile

from transactions import commit_lfs_transaction

tempfile = aiofiles.tempfile


class AsyncLFSFile(AbstractAsyncStreamedFile):
    def __init__(self, fs, path, token, host, namespace, repo_id, ref, mode="rb", **kwargs):
        super().__init__(fs=fs, path=path, mode=mode, **kwargs)
        self.path = path
        self.token = token
        self.host = host
        self.namespace = namespace
        self.repo_id = repo_id
        self.ref = ref
        self.mode = mode

        self._tmp = None
        self._shasum = sha256()
        self._changed = False
        self._downloaded = False
        self.fs = fs

    async def _ensure_tmp(self):
        if self._tmp is None:
            self._tmp = await tempfile.NamedTemporaryFile(mode="w+b", delete=True)

    async def __aenter__(self):
        await self._ensure_tmp()
        if "r" in self.mode and not self._downloaded:
            await self._download_from_gitlab()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is None:
            await self._commit()
        if self._tmp:
            await self._tmp.close()
            self._tmp = None

    async def _download_from_gitlab(self):
        data = await self.fs.gitlab_client.get_raw_file(
            repo_id=self.repo_id,
            file_path=self.path,
            ref=self.ref or "main",
            lfs=True,
        )
        await self._ensure_tmp()
        await self._tmp.write(data)
        await self._tmp.seek(0)
        self._downloaded = True

    async def read(self, length=-1):
        await self._ensure_tmp()
        if "r" in self.mode and not self._downloaded:
            await self._download_from_gitlab()
        return await self._tmp.read(length)

    async def write(self, data):
        await self._ensure_tmp()
        if isinstance(data, str):
            data = data.encode()
        self._changed = True
        self._shasum.update(data)
        return await self._tmp.write(data)

    async def _commit(self, feature_branch_prefix: str = "run_results"):
        if not self._changed:
            return

        await self._ensure_tmp()
        await self._tmp.seek(0, io.SEEK_END)
        size = await self._tmp.tell()
        await self._tmp.seek(0)
        sha = self._shasum.hexdigest()

        repo = await self.fs.gitlab_client.get_project_by_id(self.repo_id)
        if not repo:
            raise FileNotFoundError(f"Project id {self.repo_id} not found")

        await commit_lfs_transaction(
            client=self.fs.gitlab_client,
            host=self.host,
            token=str(self.token or ""),
            repo=repo,
            base_branch=self.ref,
            final_path=self.path,
            sha=sha,
            size=size,
            data_stream=self._tmp,
            feature_branch_prefix=feature_branch_prefix,
            tmp_pointer_name=True,
            create_mr=True,
        )

        if hasattr(self.fs, "_invalidate_after_write"):
            self.fs._invalidate_after_write(repo=repo, inside_path=self.path)
