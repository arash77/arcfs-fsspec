import asyncio
import io
from collections.abc import Iterable

import aiohttp
import pytest

import arcfs.async_lfs_file as async_lfs_file_module
import arcfs.fs as fs_module
import arcfs.transactions as transactions
from arcfs.async_lfs_file import AsyncLFSFile
from arcfs.fs import GitLabARCFileSystem
from arcfs.errors import RefNotFound
from arcfs.gitlab_client import GitLabClient


class FakeGitLabClient:
    def __init__(self):
        self.token = "token"
        self.closed = False
        self.root_calls: list[dict] = []
        self.project_calls: list[tuple[int, str, str]] = []
        self.project_by_path_calls: list[str] = []
        self.project_by_id_calls: list[int] = []
        self.project_page_calls: list[dict] = []
        self.root_page_calls: list[dict] = []
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

    async def retrieve_root_level_page(self, *, page, per_page, **kwargs):
        self.root_page_calls.append({"page": page, "per_page": per_page})
        # GitLab caps per_page at 100 on the projects endpoint.
        effective = min(per_page, 100)
        items = list(self.projects.values())
        start = (page - 1) * effective
        return items[start:start + effective], len(items)

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
    fs.dircache.clear()
    fs._project_index_built = False
    fs._project_index_building = False
    return fs


def names(entries: Iterable[dict]) -> list[str]:
    return [entry["name"] for entry in entries]


def test_root_params_order_projects_by_descending_id():
    client = GitLabClient("https://example.invalid", "token")

    params = client._build_root_params(
        page=1,
        per_page=100,
        membership=False,
        archived=False,
        simple=True,
    )

    assert params["order_by"] == "id"
    assert params["sort"] == "desc"


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


def _paging_fs(monkeypatch, tree_entries):
    """Filesystem wired to a fake client holding ``tree_entries`` under (1, "")."""
    fs = GitLabARCFileSystem(
        "https://example.invalid",
        "token",
        asynchronous=False,
        skip_instance_cache=True,
    )
    fs.client = FakeGitLabClient()
    fs.client.tree[(1, "")] = tree_entries

    def run_sync(loop, func, *args, **kwargs):
        return asyncio.run(func(*args, **kwargs))

    monkeypatch.setattr(fs_module, "sync", run_sync)
    return fs


def test_list_page_serves_an_unaligned_window_from_pages(monkeypatch):
    """An offset that is not a multiple of the limit must not fall back to a full listing."""
    entries = [{"path": f"f{i:02d}.txt", "type": "blob"} for i in range(10)]
    fs = _paging_fs(monkeypatch, entries)

    try:
        out, total_count = fs.list_page("group/repo1", detail=False, offset=3, limit=4, ref="main")
    finally:
        fs.close()

    assert out == [
        "group/repo1:-:f03.txt",
        "group/repo1:-:f04.txt",
        "group/repo1:-:f05.txt",
        "group/repo1:-:f06.txt",
    ]
    assert total_count == 10
    # Served from backend pages, not from a full listing of the directory.
    assert fs.client.project_page_calls
    assert fs.client.project_calls == []


def test_list_page_assembles_root_windows_larger_than_the_projects_cap(monkeypatch):
    """GitLab caps per_page at 100 on the projects endpoint, so a larger window needs two requests."""
    fs = _paging_fs(monkeypatch, [])
    fs.client.projects = {
        f"group/repo{i:03d}": {"id": i + 1, "original_path": f"group/repo{i:03d}"} for i in range(250)
    }

    try:
        out, total_count = fs.list_page("", detail=False, offset=0, limit=150)
    finally:
        fs.close()

    assert len(out) == 150
    assert len(set(out)) == 150, "pages must not repeat entries"
    assert out[0] == "group/repo000:-:"
    assert out[-1] == "group/repo149:-:"
    assert total_count == 250
    assert all(call["per_page"] <= 100 for call in fs.client.root_page_calls)
    assert [call["page"] for call in fs.client.root_page_calls] == [1, 2]


def test_list_page_assembles_large_tree_window_from_capped_pages(monkeypatch):
    """Keep tree page requests within GitLab's documented maximum."""
    entries = [{"path": f"f{i:03d}.txt", "type": "blob"} for i in range(250)]
    fs = _paging_fs(monkeypatch, entries)

    try:
        out, _ = fs.list_page("group/repo1", detail=False, offset=0, limit=150, ref="main")
    finally:
        fs.close()

    assert len(out) == 150
    assert [call["per_page"] for call in fs.client.project_page_calls] == [100, 100]


def test_list_page_past_the_end_returns_empty_with_the_real_total(monkeypatch):
    entries = [{"path": f"f{i:02d}.txt", "type": "blob"} for i in range(10)]
    fs = _paging_fs(monkeypatch, entries)

    try:
        out, total_count = fs.list_page("group/repo1", detail=False, offset=50, limit=10, ref="main")
    finally:
        fs.close()

    assert out == []
    assert total_count == 10


# ----------------------------------------------------------------------
# Totals when GitLab does not send X-Total
#
# GitLab drops X-Total once a query would return more than 10,000 records, so
# large instances omit it on exactly the listings that are most expensive to
# count. These are pure header tests against the real client.
# ----------------------------------------------------------------------


def _client():
    return GitLabClient("https://example.invalid", "token")


def test_total_or_bound_prefers_an_exact_total():
    """An exact X-Total always wins, whatever the other headers say."""
    headers = {"X-Total": "251", "X-Next-Page": "2", "X-Per-Page": "25"}
    assert _client()._total_or_bound(headers, page=1, per_page=25, item_count=25) == 251


def test_total_or_bound_reports_a_lower_bound_while_pages_follow():
    """Without a total, report what has been seen plus one so the caller keeps paging."""
    headers = {"X-Next-Page": "2", "X-Per-Page": "25"}
    assert _client()._total_or_bound(headers, page=1, per_page=25, item_count=25) == 26
    assert _client()._total_or_bound(headers, page=4, per_page=25, item_count=25) == 101


def test_total_or_bound_is_exact_on_the_last_page():
    """An empty X-Next-Page means this is the last page, so the count is exact."""
    headers = {"X-Next-Page": "", "X-Per-Page": "25"}
    assert _client()._total_or_bound(headers, page=3, per_page=25, item_count=7) == 57


def test_total_or_bound_follows_the_link_header_when_x_next_page_is_absent():
    """Either signal is enough; an instance may send only Link."""
    link = '<https://example.invalid/api/v4/projects?page=2>; rel="next"'
    headers = {"Link": link, "X-Per-Page": "25"}
    assert _client()._total_or_bound(headers, page=1, per_page=25, item_count=25) == 26

    headers = {"Link": '<https://example.invalid/api/v4/projects?page=1>; rel="first"'}
    assert _client()._total_or_bound(headers, page=1, per_page=25, item_count=25) == 25


def test_total_or_bound_reads_the_applied_per_page():
    """GitLab caps per_page and reports what it used, so trust the header over the request."""
    headers = {"X-Next-Page": "", "X-Per-Page": "100"}
    # 500 was requested, 100 applied: page 2 means 100 already seen, not 500.
    assert _client()._total_or_bound(headers, page=2, per_page=500, item_count=100) == 200


def test_total_or_bound_treats_an_unparseable_total_as_missing():
    """A garbage X-Total must degrade to the bound, not raise."""
    headers = {"X-Total": "not-a-number", "X-Next-Page": "2", "X-Per-Page": "25"}
    assert _client()._total_or_bound(headers, page=1, per_page=25, item_count=25) == 26


def test_total_or_bound_assumes_more_when_the_server_sends_no_signal():
    """With no total and no next-page signal, over-report rather than hide entries.

    Under-reporting would tell the caller the listing ends here, and a consumer
    that only offers paging when the total exceeds one page would then make
    every later entry unreachable.
    """
    headers = {"X-Per-Page": "25"}
    with pytest.warns(RuntimeWarning):
        full_page = _client()._total_or_bound(headers, page=1, per_page=25, item_count=25)
    assert full_page == 26

    with pytest.warns(RuntimeWarning):
        short_page = _client()._total_or_bound(headers, page=1, per_page=25, item_count=7)
    assert short_page == 7


def test_list_page_with_refresh_uses_the_paged_path(monkeypatch):
    """refresh=True must not disable paging.

    refresh was read with ``kwargs.get``, so it stayed in kwargs and was passed
    to _resolve both explicitly and through the splat. That raised TypeError,
    which the removed catch-all turned into a silent whole-listing fetch.
    """
    entries = [{"path": f"f{i:02d}.txt", "type": "blob"} for i in range(10)]
    fs = _paging_fs(monkeypatch, entries)

    try:
        out, total_count = fs.list_page(
            "group/repo1", detail=False, offset=0, limit=4, ref="main", refresh=True
        )
    finally:
        fs.close()

    assert len(out) == 4
    assert total_count == 10
    assert fs.client.project_page_calls, "refresh=True must still use the paged endpoint"
    assert fs.client.project_calls == [], "the whole-listing path must not be used"


def test_put_file_with_refresh_reaches_the_upload(monkeypatch, tmp_path):
    """refresh=True must not break uploads.

    _put_file had the same double-pass as _list_page: refresh was read with
    ``kwargs.get`` and then handed to _resolve both explicitly and through the
    splat. Unlike the listing path there was no catch-all here, so it raised.
    """
    fs = _paging_fs(monkeypatch, [])
    calls = []

    async def fake_upload(**kwargs):
        calls.append(kwargs)

    fs.client.upload_file_lfs = fake_upload

    local = tmp_path / "payload.txt"
    local.write_bytes(b"hello")

    fs.put_file(str(local), "group/repo1:-:assays/payload.txt", refresh=True)

    assert len(calls) == 1, "the upload must actually be reached"
    assert calls[0]["final_path"] == "assays/payload.txt"


def test_total_or_bound_reports_nothing_for_a_page_past_the_end():
    """An empty page must not be read as a count.

    ``(page - 1) * per_page`` is the offset the CALLER chose, so treating it as
    a total turns any large offset into a phantom count of entries that do not
    exist. Without an exact total an empty page supports no lower bound at all.
    """
    headers = {"X-Page": "21", "X-Per-Page": "100", "X-Next-Page": ""}
    assert _client()._total_or_bound(headers, page=21, per_page=100, item_count=0) == 0
    assert _client()._total_or_bound(headers, page=1001, per_page=100, item_count=0) == 0

    # An exact total is still honoured for the same empty page.
    exact = {"X-Total": "1500", "X-Page": "21", "X-Per-Page": "100"}
    assert _client()._total_or_bound(exact, page=21, per_page=100, item_count=0) == 1500


def test_list_page_keeps_the_tightest_bound_across_pages(monkeypatch):
    """A window whose last page is empty must keep the bound an earlier page gave.

    Reporting only the final page's value would throw away what the full pages
    already proved and collapse the total to zero.
    """
    entries = [{"path": f"f{i:04d}.txt", "type": "blob"} for i in range(150)]
    fs = _paging_fs(monkeypatch, entries)

    try:
        # Spans page 2 (50 real entries) and page 3 (empty).
        out, total_count = fs.list_page(
            "group/repo1", detail=False, offset=100, limit=200, ref="main"
        )
    finally:
        fs.close()

    assert len(out) == 50
    assert total_count == 150

# ----------------------------------------------------------------------
# fsspec cache options
# ----------------------------------------------------------------------


def _fs_with_cache_options(**options) -> GitLabARCFileSystem:
    fs = GitLabARCFileSystem(
        "https://example.invalid",
        "token",
        asynchronous=True,
        skip_instance_cache=True,
        **options,
    )
    fs.client = FakeGitLabClient()
    return fs


def test_cache_options_reach_the_cache():
    """fsspec builds the cache from these, so they have to survive __init__."""
    fs = _fs_with_cache_options(
        use_listings_cache=False, listings_expiry_time=42, max_paths=7
    )

    assert fs.dircache.use_listings_cache is False
    assert fs.dircache.listings_expiry_time == 42
    assert fs.dircache.max_paths == 7


def test_listings_cache_can_be_switched_off():
    """With caching off the cache must keep nothing, and listing must still work."""
    fs = _fs_with_cache_options(use_listings_cache=False)
    fs.dircache["group/repo1:-:"] = [{"name": "group/repo1:-:x", "type": "file"}]

    assert fs.dircache.get("group/repo1:-:") is None

    first = asyncio.run(fs._ls("", detail=False))
    second = asyncio.run(fs._ls("", detail=False))
    assert first == second, "a listing must not depend on the cache holding anything"
    assert first, "listing must still return entries with the cache disabled"


def test_listings_are_cached_by_default():
    """The default is unchanged: a second listing is served without asking GitLab again."""
    fs = _fs_with_cache_options()

    asyncio.run(fs._ls("", detail=False))
    calls_after_first = len(fs.client.root_calls)
    asyncio.run(fs._ls("", detail=False))

    assert len(fs.client.root_calls) == calls_after_first, "second listing should be cached"


def test_expired_listings_are_fetched_again():
    """An expiry of zero makes every cached listing stale, so it must be fetched again."""
    fs = _fs_with_cache_options(listings_expiry_time=0)

    asyncio.run(fs._ls("group/repo1", detail=False))
    calls_after_first = len(fs.client.project_calls)
    asyncio.run(fs._ls("group/repo1", detail=False))

    assert len(fs.client.project_calls) > calls_after_first, "expired listing should be refetched"


def test_expiry_does_not_reach_the_root_project_index():
    """The root listing is held twice, and the expiry only governs one of them.

    The directory cache does expire, but the entries are rebuilt from ``self.repos``,
    which ``_ensure_project_index`` keeps behind its own ``_project_index_built`` flag.
    fsspec's options say nothing about that second cache, so the root is not refetched.
    ``refresh=True`` is what rebuilds it.
    """
    fs = _fs_with_cache_options(listings_expiry_time=0)

    asyncio.run(fs._ls("", detail=False))
    calls_after_first = len(fs.client.root_calls)

    asyncio.run(fs._ls("", detail=False))
    assert fs.dircache.get("__root__") is None, "the directory cache should have expired"
    assert len(fs.client.root_calls) == calls_after_first, "but the project index is reused"

    asyncio.run(fs._ls("", detail=False, refresh=True))
    assert len(fs.client.root_calls) > calls_after_first, "refresh rebuilds the index"


# ----------------------------------------------------------------------
# Telling GitLab's two 404s apart, and reading what it said
# ----------------------------------------------------------------------
class FakeResponse:
    """Just enough of an aiohttp response for the paths that read the body."""

    def __init__(self, status, body, *, content_type="application/json"):
        self.status = status
        self._body = body
        self.headers: dict = {}
        self.request_info = None
        self.history = ()
        self.content_type = content_type

    async def json(self):
        if self.content_type != "application/json":
            raise aiohttp.ContentTypeError(None, ())
        return self._body

    def raise_for_status(self):
        if self.status >= 400:
            raise aiohttp.ClientResponseError(
                self.request_info, self.history, status=self.status, message="Bad Request"
            )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class FakeSession:
    """Session answering every request with one prepared response."""

    def __init__(self, response):
        self._response = response
        self.calls: list[tuple[str, str]] = []

    def get(self, url, **kwargs):
        self.calls.append(("get", url))
        return self._response

    def post(self, url, **kwargs):
        self.calls.append(("post", url))
        return self._response


def _client_answering(response):
    client = GitLabClient("https://example.invalid", "token")

    async def ensure():
        return FakeSession(response)

    client._ensure = ensure
    return client


def test_a_missing_ref_is_not_reported_as_a_missing_file():
    """GitLab answers 404 for both and only the body says which.

    A caller choosing between creating and updating reads a missing ref as "no
    such file" and goes on to commit against a branch that is not there, which
    GitLab refuses with a 400 that explains nothing.
    """
    client = _client_answering(FakeResponse(404, {"message": "404 Commit Not Found"}))

    with pytest.raises(RefNotFound):
        asyncio.run(client.get_file(1, "README.md", "no-such-branch"))


def test_a_missing_file_is_still_a_missing_file():
    client = _client_answering(FakeResponse(404, {"message": "404 File Not Found"}))

    with pytest.raises(FileNotFoundError) as caught:
        asyncio.run(client.get_file(1, "nope.md", "main"))

    assert not isinstance(caught.value, RefNotFound)


def test_a_missing_ref_is_still_catchable_as_a_missing_file():
    """Callers written against earlier versions catch FileNotFoundError.

    Making the new exception a subclass is what keeps them working, and a
    repository with no commits reports a missing ref for its own default
    branch, where treating it as "nothing to replace" is right.
    """
    assert issubclass(RefNotFound, FileNotFoundError)


def test_gitattributes_does_not_commit_against_a_branch_that_is_not_there():
    """The caller named in the report: it catches FileNotFoundError and creates.

    RefNotFound subclasses that, so without re-raising it here the fix would
    change nothing for the one place in the package that hits it.
    """
    commits: list = []

    class Client:
        async def get_file(self, repo_id, path, ref):
            raise RefNotFound(ref)

        async def create_commit(self, *args, **kwargs):
            commits.append(args)

    with pytest.raises(RefNotFound):
        asyncio.run(
            transactions.update_gitattributes(
                client=Client(), repo_id=1, branch="gone", path_str="assays/a.txt"
            )
        )

    assert commits == [], "nothing may be committed onto a branch that is not there"
