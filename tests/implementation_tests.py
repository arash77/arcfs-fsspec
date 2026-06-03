import os
import asyncio

from dotenv import load_dotenv

from arcfs.fs import GitLabARCFileSystem

BASE_URL = "https://git.nfdi4plants.org"

def raw_repo_to_marker(raw_repo: str) -> str:
    parts = [p for p in raw_repo.strip("/").split("/") if p]
    if len(parts) < 2:
        raise ValueError(
            "TEST_REPO must look like 'group/repo' or 'group/subgroup/repo'"
        )
    namespace = "/".join(parts[:-1])
    repo = parts[-1]
    return f"{namespace}:-:{repo}"


def marker_join(marker_repo: str, inside: str) -> str:
    inside = inside.strip("/")
    return f"{marker_repo}/{inside}" if inside else marker_repo


def print_block(title: str, value):
    print(f"\n=== {title} ===")
    print(value)


def test_sync(base_url: str, token: str | None, repo_raw: str, repo_subdir: str):
    fs = GitLabARCFileSystem(
        base_url,
        token,
        asynchronous=False,
    )

    repo_marker = raw_repo_to_marker(repo_raw)

    try:
        print_block("sync root ls", fs.ls("", detail=False))
        print_block("sync root ls (refresh)", fs.ls("/", detail=False, refresh=True))

        print_block("sync repo raw ls", fs.ls(repo_raw, detail=False))
        print_block("sync repo marker ls", fs.ls(repo_marker, detail=False))
        print_block("sync repo raw ls (refresh)", fs.ls(repo_raw, detail=False, refresh=True))

        if repo_subdir:
            raw_subdir = f"{repo_raw}/{repo_subdir.strip('/')}"
            marker_subdir = marker_join(repo_marker, repo_subdir)

            print_block("sync subdir raw ls", fs.ls(raw_subdir, detail=False))
            print_block("sync subdir marker ls", fs.ls(marker_subdir, detail=False))
            print_block(
                "sync subdir raw ls (refresh)",
                fs.ls(raw_subdir, detail=False, refresh=True),
            )

        detail_listing = fs.ls(repo_raw, detail=True)
        print_block("sync repo raw ls (detail=True)", detail_listing[:5])

    finally:
        # keep simple for now
        asyncio.run(fs._close())


async def test_async(base_url: str, token: str | None, repo_raw: str, repo_subdir: str):
    fs = GitLabARCFileSystem(
        base_url,
        token,
        asynchronous=True,
    )

    repo_marker = raw_repo_to_marker(repo_raw)

    try:
        root_listing = await fs._ls("", detail=False)
        print_block("async root ls", root_listing)
        print_block("async root len", len(root_listing))

        print_block("async root ls (refresh)", await fs._ls("/", detail=False, refresh=True))

        print_block("async repo raw ls", await fs._ls(repo_raw, detail=False))
        print_block("async repo marker ls", await fs._ls(repo_marker, detail=False))
        print_block(
            "async repo raw ls (refresh)",
            await fs._ls(repo_raw, detail=False, refresh=True),
        )

        if repo_subdir:
            raw_subdir = f"{repo_raw}/{repo_subdir.strip('/')}"
            marker_subdir = marker_join(repo_marker, repo_subdir)

            print_block("async subdir raw ls", await fs._ls(raw_subdir, detail=False))
            print_block("async subdir marker ls", await fs._ls(marker_subdir, detail=False))
            print_block(
                "async subdir raw ls (refresh)",
                await fs._ls(raw_subdir, detail=False, refresh=True),
            )

        detail_listing = await fs._ls(repo_raw, detail=True)
        print_block("async repo raw ls (detail=True)", detail_listing[:5])

    finally:
        await fs._close()


if __name__ == "__main__":
    load_dotenv()

    token = os.getenv("GITLAB_TOKEN")
    repo_raw = os.getenv("GITLAB_TEST_REPO", "julian.weidhase/test12345")
    repo_subdir = os.getenv("GITLAB_TEST_SUBDIR", "")

    print(f"BASE_URL={BASE_URL}")
    print(f"TEST_REPO={repo_raw}")
    print(f"TEST_SUBDIR={repo_subdir!r}")

    test_sync(BASE_URL, token, repo_raw, repo_subdir)
    asyncio.run(test_async(BASE_URL, token, repo_raw, repo_subdir))
