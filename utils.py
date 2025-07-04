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
    """
    Return the top-level repo name from a given path using pathlib.
    
    This does not give the real repositrory Path as it is in GitLab, since this
    would not be a traditionally valid path and unsupported by fsspec.
    So it is assumed repo name is the first component in a flattened directory structure:
    e.g., 'group-subgroup-repo/file.txt' → 'group-subgroup-repo'
    """
    path = Path(path).as_posix().lstrip("/")
    return path.split("/", 1)[0]


def is_direct_subdir(parent: str, child: str, resolve_symlinks: bool = False) -> bool:
    """
    Check if `child` is a direct subdirectory of `parent`.

    Parameters
    ----------
    parent : str
        The parent directory path.
    child : str
        The child directory path.
    resolve_symlinks : bool, optional (default=False)
        If True, resolve symlinks and normalize paths with `.resolve()`.
        If False, use `.absolute()` to get an absolute path without resolving symlinks.
    """
    if resolve_symlinks:
        parent_path = Path(parent).resolve(strict=False)
        child_path = Path(child).resolve(strict=False)
    else:
        parent_path = Path(parent).absolute()
        child_path = Path(child).absolute()
    try:
        # Ensure child_path is within parent_path; otherwise, it's not a subdirectory.
        # relative_to will raise ValueError if child_path is not under parent_path.
        child_path.relative_to(parent_path)
    except ValueError:
        return False
    # path.parent could in some cases be true for the same path.
    return parent_path != child_path and child_path.parent == parent_path


def prefix_path(path_prefix, path):
    """
    Prefix a path with a given path prefix.
    """
    return "/" + path_prefix + "/" + path


def split_base_url(base_url: str):
    """
    Split a GitLab base URL into its host and namespace components.

    Parameters
    ----------
    base_url : str
        The base URL of a GitLab instance or project group, e.g.,
        "https://gitlab.example.com/mygroup/subgroup".

    Returns
    -------
    tuple[str, str]
        A tuple containing:
        - host: the domain of the GitLab instance (e.g., "gitlab.example.com")
        - namespace: the path portion, stripped of leading/trailing slashes 
          (e.g., "mygroup/subgroup"). Returns an empty string if no namespace is present.

    Examples
    --------
    >>> split_base_url("https://gitlab.com/namespace/project")
    ('gitlab.com', 'namespace/project')

    >>> split_base_url("https://gitlab.example.com/")
    ('gitlab.example.com', '')
    """
    parsed = urlparse(base_url)
    host = parsed.netloc
    path = parsed.path.strip("/")  # Remove leading/trailing slashes
    return host, path

async def calculate_sha256(file_path: str) -> str:
    """
    Calculate the SHA-256 hash of a file.

    Parameters
    ----------
    file_path : str
        The path to the file for which to calculate the SHA-256 hash.

    Returns
    -------
    str
        The SHA-256 hash of the file as a hexadecimal string.
    """
    # Step 1: Compute SHA-256 OID
    sha256 = hashlib.sha256()
    async with aiofiles.open(file_path, "rb") as f:
        while True:
            chunk = await f.read(8192)
            if not chunk:
                break
            sha256.update(chunk)
    oid = sha256.hexdigest()
    return oid
