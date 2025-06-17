from pathlib import Path
from urllib.parse import urlparse

def split_first(path: Path | str):
    """
    Return a (head, tail) tuple where
    • head  – the first component of *path*
    • tail  – everything that remains after the head
    Both elements are `Path` objects.

    Examples
    --------
    >>> split_first("docs/index.html")
    (PosixPath('docs'), PosixPath('index.html'))

    >>> split_first("/usr/local/bin/python")
    (PosixPath('/usr'), PosixPath('local/bin/python'))

    >>> split_first(r"C:\\Windows\\System32\\drivers\\etc\\hosts")
    (WindowsPath('C:\\Windows'), WindowsPath('System32/drivers/etc/hosts'))
    """
    p = Path(path)
    drive = p.drive
    parts = p.parts[len(drive and (drive,)) :]

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
    """
    Return the top-level repo name from a given path using pathlib.

    Assumes repo name is the first component in a flattened directory structure:
    e.g., 'group-subgroup-repo/file.txt' → 'group-subgroup-repo'
    """
    path = Path(path).as_posix().lstrip("/")
    return path.split("/", 1)[0]


def is_direct_subdir(parent: str, child: str) -> bool:
    parent_path = Path(parent).resolve()
    child_path = Path(child).resolve()
    try:
        child_path.relative_to(parent_path)
    except ValueError:
        return False
    return parent_path != child_path and child_path.parent == parent_path


def prefix_path(path_prefix, path):
    return "/" + path_prefix + "/" + path


def split_base_url(base_url: str):
    """
    Split a GitLab base URL into host and namespace.
    """
    parsed = urlparse(base_url)
    host = parsed.netloc
    path = parsed.path.strip("/")  # Remove leading/trailing slashes
    return host, path
