from pathlib import Path
from urllib.parse import urlparse
import hashlib
import aiofiles


def split_first(path: Path | str):
    """
    Return a (head, tail) tuple where
    • head  – the first component of *path*
    • tail  – everything that remains after the head
    Both elements are `Path` objects.
    """
    p = Path(path)
    drive = p.drive
    parts = p.parts[1:] if drive else p.parts

    if not parts:
        return Path(p), Path()

    if p.is_absolute() and not drive:
        head = Path("/", parts[0])
        tail = Path(*parts[1:])
    else:
        head = Path(drive, parts[0]) if drive else Path(parts[0])
        tail = Path(*parts[1:])

    return head, tail


def get_repo_name_from_path(path: str) -> str:
    path = Path(path).as_posix().lstrip("/")
    return path.split("/", 1)[0]


def is_direct_subdir(parent: str, child: str, resolve_symlinks: bool = False) -> bool:
    if resolve_symlinks:
        parent_path = Path(parent).resolve(strict=False)
        child_path = Path(child).resolve(strict=False)
    else:
        parent_path = Path(parent).absolute()
        child_path = Path(child).absolute()
    try:
        child_path.relative_to(parent_path)
    except ValueError:
        return False
    return parent_path != child_path and child_path.parent == parent_path


def prefix_path(path_prefix, path):
    return "/" + path_prefix + "/" + path


def split_base_url(base_url: str):
    parsed = urlparse(base_url)
    host = parsed.netloc
    path = parsed.path.strip("/")
    return host, path


async def calculate_sha256(file_path: str) -> str:
    sha256 = hashlib.sha256()
    async with aiofiles.open(file_path, "rb") as f:
        while True:
            chunk = await f.read(8192)
            if not chunk:
                break
            sha256.update(chunk)
    oid = sha256.hexdigest()
    return oid