import asyncio
import io
from collections.abc import Iterable

import pytest

import arcfs.async_lfs_file as async_lfs_file_module
import arcfs.fs as fs_module
from arcfs.async_lfs_file import AsyncLFSFile
from arcfs.fs import GitLabARCFileSystem


class FakeGitLabClient:
    def __init__(self):
        self.token = "token"
        self.closed = False
        self.root_calls: list[dict] = []
        self.project_calls: list[tuple[int, str, str]] = []
        self.project_by_path_calls: list[str] = []
        self.project_by_id_calls: list[int] = []
        self.project_page_calls: list[dict] = []
        self.stream_calls: list[dict] = []

        self.projects = {
            "group/repo1": {"id": 1, "original_path": "group/repo1"},
            "group/sub/repo2": {"id": 2, "original_path": "group/sub/repo2"},
            "group/sub/deeper/repo3": {
                "id": 3,
                "original_path": "group/sub/deeper/repo3",
            },
            "other/repo4": {"id": 4, "original_path": "other/repo4"},
        }

        self.tree = {
            (1, ""): [
                {"path": "README.md", "type": "blob"},
                {"path": "docs", "type": "tree"},
                {"path": "nested", "type": "tree"},
            ],
            (1, "docs"): [
                {"path": "docs/guide.md", "type": "blob"},
                {"path": "docs/tutorial.txt", "type": "blob"},
            ],
            (1, "nested"): [{"path": "nested/leaf.txt", "type": "blob"}],
            (2, ""): [
                {"path": "subdir", "type": "tree"},
                {"path": "notes.txt", "type": "blob"},
            ],
            (2, "subdir"): [{"path": "subdir/file.txt", "type": "blob"}],
            (3, ""): [{"path": "deep.txt", "type": "blob"}],
            (4, ""): [{"path": "top.csv", "type": "blob"}],
        }

        self.files = {
            (1, "README.md", "main"): b"hello from main\n",
            (1, "README.md", "feature"): b"hello from feature\n",
        }

    async def retrieve_root_level(self, **kwargs):
        self.root_calls.append(dict(kwargs))
        return list(self.projects.values())

    async def retrieve_project_level(self, repo_id, subdir, *, ref="main", per_page=100):
        self.project_calls.append((repo_id, subdir, ref))
        key = (repo_id, subdir)
        if key not in self.tree:
            raise FileNotFoundError(f"No such directory in fake tree: {key}")
        return list(self.tree[key])

    async def retrieve_project_level_page(
        self,
        *,
        repo_id,
        subdir,
        ref,
        page,
        per_page,
    ):
        self.project_page_calls.append(
            {
                "repo_id": repo_id,
                "subdir": subdir,
                "ref": ref,
                "page": page,
                "per_page": per_page,
            }
        )
        items = self.tree[(repo_id, subdir)]
        start = (page - 1) * per_page
        return items[start:start + per_page], len(items)

    async def get_project_by_path(self, path_with_namespace):
        self.project_by_path_calls.append(path_with_namespace)
        return self.projects.get(path_with_namespace)

    async def get_project_by_id(self, repo_id):
        self.project_by_id_calls.append(repo_id)
        for project in self.projects.values():
            if project["id"] == repo_id:
                return project
        return None

    async def close(self):
        self.closed = True

    async def get_default_branch(self, repo_id):
        return "main"

    async def stream_file(self, repo_id, path, ref, chunk_size=1024 * 1024):
        self.stream_calls.append(
            {
                "repo_id": repo_id,
                "path": path,
                "ref": ref,
                "chunk_size": chunk_size,
            }
        )
        data = self.files[(repo_id, path, ref)]
        for offset in range(0, len(data), chunk_size):
            yield data[offset:offset + chunk_size]


@pytest.fixture
def fs() -> GitLabARCFileSystem:
    fs = GitLabARCFileSystem(
        "https://example.invalid",
        "token",
        asynchronous=True,
        skip_instance_cache=True,
    )
    fs.client = FakeGitLabClient()
    fs.repos = {}
    fs.not_repo = set()
    fs.dircache = {}
    fs._project_index_built = False
    fs._project_index_building = False
    return fs


def names(entries: Iterable[dict]) -> list[str]:
    return [entry["name"] for entry in entries]


@pytest.fixture
def fake_lfs_tempfile(monkeypatch):
    class FakeAsyncTempFile:
        def __init__(self):
            self._file = io.BytesIO()

        async def write(self, data):
            return self._file.write(data)

        async def read(self, length=-1):
            return self._file.read(length)

        async def seek(self, offset, whence=0):
            return self._file.seek(offset, whence)

        async def tell(self):
            return self._file.tell()

        async def close(self):
            self._file.close()

    class FakeTempfileModule:
        async def NamedTemporaryFile(self, mode="w+b", delete=True):
            return FakeAsyncTempFile()

    monkeypatch.setattr(async_lfs_file_module, "tempfile", FakeTempfileModule())


@pytest.mark.asyncio
async def test_open_read_returns_async_lfs_file(fs: GitLabARCFileSystem):
    opened = await fs.open_async("group/repo1/README.md", mode="rb")

    assert isinstance(opened, AsyncLFSFile)
    assert opened.path == "README.md"
    assert opened.repo_id == 1
    assert opened.token == "token"
    assert opened.mode == "rb"
    await opened.close()


@pytest.mark.asyncio
async def test_open_repo_root_raises_is_directory(fs: GitLabARCFileSystem):
    with pytest.raises(IsADirectoryError):
        await fs.open_async("group/repo1", mode="rb")


@pytest.mark.asyncio
async def test_open_read_lazily_downloads_content(
    fs: GitLabARCFileSystem,
    fake_lfs_tempfile,
):
    opened = await fs.open_async("group/repo1/README.md", mode="rb")

    assert fs.client.stream_calls == []
    assert await opened.read() == b"hello from main\n"
    assert fs.client.stream_calls == [
        {
            "repo_id": 1,
            "path": "README.md",
            "ref": "main",
            "chunk_size": 1024 * 1024,
        }
    ]
    await opened.close()


@pytest.mark.asyncio
async def test_open_read_preserves_explicit_ref(
    fs: GitLabARCFileSystem,
    fake_lfs_tempfile,
):
    opened = await fs.open_async("group/repo1/README.md", mode="rb", ref="feature")

    assert await opened.read() == b"hello from feature\n"
    assert fs.client.stream_calls[-1]["ref"] == "feature"
    await opened.close()


@pytest.mark.asyncio
async def test_open_write_commits_changed_content_on_successful_exit(
    fs: GitLabARCFileSystem,
    monkeypatch,
    fake_lfs_tempfile,
):
    commits = []

    async def fake_commit_lfs_transaction(**kwargs):
        data = await kwargs["data_stream"].read()
        commits.append({**kwargs, "data": data})

    monkeypatch.setattr(
        async_lfs_file_module,
        "commit_lfs_transaction",
        fake_commit_lfs_transaction,
    )

    async with await fs.open_async("group/repo1/output.bin", mode="wb", ref="main") as f:
        await f.write(b"new data")

    assert len(commits) == 1
    assert commits[0]["client"] is fs.client
    assert commits[0]["token"] == "token"
    assert commits[0]["repo"] == {"id": 1, "original_path": "group/repo1"}
    assert commits[0]["base_branch"] == "main"
    assert commits[0]["final_path"] == "output.bin"
    assert commits[0]["size"] == len(b"new data")
    assert commits[0]["data"] == b"new data"


@pytest.mark.asyncio
async def test_open_write_unchanged_session_does_not_commit(
    fs: GitLabARCFileSystem,
    monkeypatch,
    fake_lfs_tempfile,
):
    commits = []

    async def fake_commit_lfs_transaction(**kwargs):
        commits.append(kwargs)

    monkeypatch.setattr(
        async_lfs_file_module,
        "commit_lfs_transaction",
        fake_commit_lfs_transaction,
    )

    async with await fs.open_async("group/repo1/output.bin", mode="wb"):
        pass

    assert commits == []


def test_open_sync_hook_returns_async_lfs_file():
    fs = GitLabARCFileSystem(
        "https://example.invalid",
        "token",
        asynchronous=False,
        skip_instance_cache=True,
    )
    fs.client = FakeGitLabClient()

    try:
        opened = fs._open("group/repo1/README.md", mode="rb")
        assert isinstance(opened, AsyncLFSFile)
        assert opened.path == "README.md"
        asyncio.run(opened.close())
    finally:
        fs.close()


@pytest.mark.asyncio
async def test_root_listing_detail_false(fs: GitLabARCFileSystem):
    out = await fs._ls("", detail=False)

    assert set(out) == {
        "group/repo1:-:",
        "group/sub/repo2:-:",
        "group/sub/deeper/repo3:-:",
        "other/repo4:-:",
    }


@pytest.mark.asyncio
async def test_root_listing_detail_true(fs: GitLabARCFileSystem):
    out = await fs._ls("/", detail=True)

    assert {tuple(sorted(item.items())) for item in out} == {
        tuple(sorted({"name": "group/repo1:-:", "type": "directory"}.items())),
        tuple(sorted({"name": "group/sub/repo2:-:", "type": "directory"}.items())),
        tuple(sorted({"name": "group/sub/deeper/repo3:-:", "type": "directory"}.items())),
        tuple(sorted({"name": "other/repo4:-:", "type": "directory"}.items())),
    }


@pytest.mark.asyncio
async def test_root_listing_uses_cache_without_refresh(fs: GitLabARCFileSystem):
    await fs._ls("", detail=False)
    await fs._ls("/", detail=False)

    assert len(fs.client.root_calls) == 1


@pytest.mark.asyncio
async def test_root_listing_refresh_rebuilds_project_index(fs: GitLabARCFileSystem):
    first = await fs._ls("", detail=False)
    assert "group/repo1:-:" in first

    del fs.client.projects["group/repo1"]

    refreshed = await fs._ls("", detail=False, refresh=True)
    assert "group/repo1:-:" not in refreshed
    assert len(fs.client.root_calls) == 2


@pytest.mark.asyncio
async def test_root_listing_uses_root_index_fetch_defaults(fs: GitLabARCFileSystem):
    await fs._ls("", detail=False)

    assert fs.client.root_calls[-1] == {"per_page": 100, "simple": True}


@pytest.mark.asyncio
async def test_raw_repo_root_listing(fs: GitLabARCFileSystem):
    out = await fs._ls("group/repo1", detail=False)

    assert set(out) == {
        "group/repo1:-:README.md",
        "group/repo1:-:docs",
        "group/repo1:-:nested",
    }


@pytest.mark.asyncio
async def test_marker_repo_root_listing(fs: GitLabARCFileSystem):
    out = await fs._ls("group/repo1:-:", detail=False)

    assert set(out) == {
        "group/repo1:-:README.md",
        "group/repo1:-:docs",
        "group/repo1:-:nested",
    }


@pytest.mark.asyncio
async def test_subgroup_repo_listing_raw(fs: GitLabARCFileSystem):
    out = await fs._ls("group/sub/repo2", detail=False)

    assert set(out) == {
        "group/sub/repo2:-:subdir",
        "group/sub/repo2:-:notes.txt",
    }


@pytest.mark.asyncio
async def test_deep_subgroup_repo_listing_raw(fs: GitLabARCFileSystem):
    out = await fs._ls("group/sub/deeper/repo3", detail=False)

    assert out == ["group/sub/deeper/repo3:-:deep.txt"]


@pytest.mark.asyncio
async def test_subdir_listing_raw(fs: GitLabARCFileSystem):
    out = await fs._ls("group/repo1/docs", detail=False)

    assert set(out) == {
        "group/repo1:-:docs/guide.md",
        "group/repo1:-:docs/tutorial.txt",
    }


@pytest.mark.asyncio
async def test_subdir_listing_marker(fs: GitLabARCFileSystem):
    out = await fs._ls("group/repo1:-:/docs", detail=False)

    assert set(out) == {
        "group/repo1:-:docs/guide.md",
        "group/repo1:-:docs/tutorial.txt",
    }


@pytest.mark.asyncio
async def test_path_normalization(fs: GitLabARCFileSystem):
    out = await fs._ls("  ///group/repo1///docs///  ", detail=False)

    assert set(out) == {
        "group/repo1:-:docs/guide.md",
        "group/repo1:-:docs/tutorial.txt",
    }


@pytest.mark.asyncio
async def test_non_root_listing_uses_cache_without_refresh(fs: GitLabARCFileSystem):
    await fs._ls("group/repo1", detail=False)
    await fs._ls("group/repo1", detail=False)

    assert len(fs.client.project_calls) == 1


@pytest.mark.asyncio
async def test_non_root_listing_refresh_refetches(fs: GitLabARCFileSystem):
    await fs._ls("group/repo1", detail=False)
    await fs._ls("group/repo1", detail=False, refresh=True)

    assert len(fs.client.project_calls) == 2


@pytest.mark.asyncio
async def test_detail_true_returns_dicts(fs: GitLabARCFileSystem):
    out = await fs._ls("group/repo1", detail=True)

    assert all(isinstance(item, dict) for item in out)
    assert {item["type"] for item in out} == {"file", "directory"}
    assert "group/repo1:-:README.md" in names(out)


@pytest.mark.asyncio
async def test_not_found_raises_for_unknown_project(fs: GitLabARCFileSystem):
    with pytest.raises(FileNotFoundError):
        await fs._ls("missing/repo", detail=False)


@pytest.mark.asyncio
async def test_not_found_raises_for_unknown_marker_project(fs: GitLabARCFileSystem):
    with pytest.raises(FileNotFoundError):
        await fs._ls("missing:-:repo", detail=False)


@pytest.mark.asyncio
async def test_not_found_raises_for_unknown_subdir(fs: GitLabARCFileSystem):
    with pytest.raises(FileNotFoundError):
        await fs._ls("group/repo1/nope", detail=False)


@pytest.mark.asyncio
async def test_resolve_raw_fallback_builds_project_index(fs: GitLabARCFileSystem):
    with pytest.raises(FileNotFoundError):
        await fs._ls("totally/missing/path", detail=False)

    assert fs.client.root_calls[-1] == {"per_page": 100, "simple": True}


@pytest.mark.asyncio
async def test_negative_cache_is_populated_for_misses(fs: GitLabARCFileSystem):
    with pytest.raises(FileNotFoundError):
        await fs._ls("totally/missing/path", detail=False)

    assert "totally/missing/path" in fs.not_repo


@pytest.mark.asyncio
async def test_rm_file_disabled(fs: GitLabARCFileSystem):
    with pytest.raises(PermissionError):
        await fs._rm_file("group/repo1/README.md")


@pytest.mark.asyncio
async def test_rm_disabled(fs: GitLabARCFileSystem):
    with pytest.raises(PermissionError):
        await fs._rm("group/repo1", recursive=True)


@pytest.mark.asyncio
async def test_close_closes_client(fs: GitLabARCFileSystem):
    await fs._close()
    assert fs.client.closed is True


def test_close_sync_wrapper_closes_client(monkeypatch):
    fs = GitLabARCFileSystem(
        "https://example.invalid",
        "token",
        asynchronous=False,
        skip_instance_cache=True,
    )
    fs.client = FakeGitLabClient()
    sync_calls = []

    def run_sync(loop, func, *args, **kwargs):
        sync_calls.append({"loop": loop, "func": func.__name__})
        return asyncio.run(func(*args, **kwargs))

    monkeypatch.setattr(fs_module, "sync", run_sync)

    fs.close()

    assert fs.client.closed is True
    assert sync_calls == [{"loop": fs.loop, "func": "_close"}]


def test_list_page_sync_wrapper_calls_async_list_page(monkeypatch):
    fs = GitLabARCFileSystem(
        "https://example.invalid",
        "token",
        asynchronous=False,
        skip_instance_cache=True,
    )
    fs.client = FakeGitLabClient()
    sync_calls = []
    calls = []
    expected = ([{"name": "group/repo1:-:README.md", "type": "file"}], 3)

    def run_sync(loop, func, *args, **kwargs):
        sync_calls.append({"loop": loop, "func": func.__name__})
        return asyncio.run(func(*args, **kwargs))

    async def fake_list_page(path, detail=True, *, offset=0, limit=50, **kwargs):
        calls.append(
            {
                "path": path,
                "detail": detail,
                "offset": offset,
                "limit": limit,
                "kwargs": kwargs,
            }
        )
        return expected

    monkeypatch.setattr(fs_module, "sync", run_sync)
    fs._list_page = fake_list_page

    try:
        out = fs.list_page("group/repo1", detail=True, offset=2, limit=1, ref="main")
    finally:
        fs.close()

    assert out == expected
    assert sync_calls[0] == {"loop": fs.loop, "func": "fake_list_page"}
    assert calls == [
        {
            "path": "group/repo1",
            "detail": True,
            "offset": 2,
            "limit": 1,
            "kwargs": {"ref": "main"},
        }
    ]


def test_list_page_sync_wrapper_returns_real_page(monkeypatch):
    fs = GitLabARCFileSystem(
        "https://example.invalid",
        "token",
        asynchronous=False,
        skip_instance_cache=True,
    )
    fs.client = FakeGitLabClient()

    def run_sync(loop, func, *args, **kwargs):
        return asyncio.run(func(*args, **kwargs))

    monkeypatch.setattr(fs_module, "sync", run_sync)

    try:
        out, total_count = fs.list_page(
            "group/repo1",
            detail=False,
            offset=1,
            limit=1,
            ref="main",
        )
    finally:
        fs.close()

    assert out == ["group/repo1:-:docs"]
    assert total_count == 3
    assert fs.client.project_page_calls == [
        {
            "repo_id": 1,
            "subdir": "",
            "ref": "main",
            "page": 2,
            "per_page": 1,
        }
    ]


def test_sync_wrapper_smoke():
    fs = GitLabARCFileSystem(
        "https://example.invalid",
        "token",
        asynchronous=False,
        skip_instance_cache=True,
    )
    fs.client = FakeGitLabClient()

    try:
        out = fs.ls("group/repo1", detail=False)
        assert set(out) == {
            "group/repo1:-:README.md",
            "group/repo1:-:docs",
            "group/repo1:-:nested",
        }
    finally:
        asyncio.run(fs._close())
