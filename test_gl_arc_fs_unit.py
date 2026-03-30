import asyncio
from collections.abc import Iterable

import pytest

from gl_arc_fs import GitLabARCFileSystem


class FakeGitLabClient:
    def __init__(self):
        self.closed = False
        self.root_calls: list[dict] = []
        self.project_calls: list[tuple[int, str, str]] = []
        self.project_by_path_calls: list[str] = []

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

    async def retrieve_root_level(self, **kwargs):
        self.root_calls.append(dict(kwargs))
        return list(self.projects.values())

    async def retrieve_project_level(self, repo_id, subdir, *, ref="main", per_page=100):
        self.project_calls.append((repo_id, subdir, ref))
        key = (repo_id, subdir)
        if key not in self.tree:
            raise FileNotFoundError(f"No such directory in fake tree: {key}")
        return list(self.tree[key])

    async def get_project_by_path(self, path_with_namespace):
        self.project_by_path_calls.append(path_with_namespace)
        return self.projects.get(path_with_namespace)

    async def close(self):
        self.closed = True


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
async def test_root_listing_passes_concurrent_offset(fs: GitLabARCFileSystem):
    await fs._ls("", detail=False, concurrent_offset=True)

    assert fs.client.root_calls[-1]["concurrent_offset"] is True


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
    out = await fs._ls("group:-:repo1", detail=False)

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
    out = await fs._ls("group:-:repo1/docs", detail=False)

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
async def test_resolve_raw_fallback_passes_concurrent_offset(fs: GitLabARCFileSystem):
    with pytest.raises(FileNotFoundError):
        await fs._ls("totally/missing/path", detail=False, concurrent_offset=True)

    assert fs.client.root_calls[-1]["concurrent_offset"] is True


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
