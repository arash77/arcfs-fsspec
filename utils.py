from __future__ import annotations

from pathlib import Path, PurePosixPath
import hashlib
import aiofiles


def split_first(path: Path | str):
    """
    Split a path into (head, tail).

    Example:
        split_first("a/b/c") -> ("a", "b/c")
    """
    p = Path(path)
    parts = p.parts
    if not parts:
        return Path(""), Path("")
    return Path(parts[0]), Path(*parts[1:])


def norm_inside(inside: str | None) -> str:
    """
    Normalize a repository-internal path.

    - POSIX-style
    - no leading slash
    - no trailing slash
    - empty or "." becomes ""

    Examples:
        norm_inside("/a/b/") -> "a/b"
        norm_inside("")      -> ""
        norm_inside(None)    -> ""
    """
    if not inside:
        return ""
    norm = str(PurePosixPath(str(inside))).strip("/")
    return "" if norm in ("", ".") else norm


async def calculate_sha256(file_path: str) -> str:
    """
    Asynchronously calculate the SHA256 checksum of a local file.

    Used for LFS pointer creation.
    """
    sha = hashlib.sha256()
    async with aiofiles.open(file_path, "rb") as f:
        while True:
            chunk = await f.read(8192)
            if not chunk:
                break
            sha.update(chunk)
    return sha.hexdigest()


def lfs_pointer_text(sha: str, size: int) -> str:
    """
    Generate the exact Git LFS pointer file content.
    """
    return (
        "version https://git-lfs.github.com/spec/v1\n"
        f"oid sha256:{sha}\n"
        f"size {size}\n"
    )


def legacy_gitattributes_block(path_str: str) -> str:
    """
    Generate a .gitattributes block compatible with the legacy ARC/LFS layout.

    IMPORTANT:
    This mirrors your old pyfilesystem-based implementation byte-for-byte.
    """
    return (
        "# Leave following line: auto generated\n"
        f"{path_str} filter=lfs diff=lfs merge=lfs -text \n"
    )
