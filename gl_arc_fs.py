import fsspec
import fsspec.asyn
import asyncio
from pathlib import Path, PurePosixPath
from dotenv import load_dotenv
import os
from async_lfs_file import AsyncLFSFile
from fsspec.asyn import AbstractAsyncStreamedFile
from utils import split_first
from gitlab_client import GitLabClient


class GitLab_ARC_FileSystem(fsspec.asyn.AsyncFileSystem):
    """
    Async GitLab-backed filesystem with Git-LFS support.
    ----------

    Internals
    ---------
    • **Direct-path seeding**: address a repo and subpaths without listing root.
    • **Dual-path querying**: use either canonical `group/sub/repo/...` or a
      collision-free flat key using `sep` (default `'::'`):
      `group::sub::repo/...`.
    • `dircache` stores *only* the flat key form. Two-way maps allow lookups.
    """

    def __init__(self, base_url, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._base_url = base_url.rstrip("/")
        self.token = kwargs.get("token")
        self.semaphore = asyncio.Semaphore(kwargs.get("semaphore", 25))
        self.per_page = kwargs.get("per_page", 100)
        self.use_cache = kwargs.get("use_cache", True)
        self._cache_type = "dir"
        self.gitlab_client = GitLabClient(base_url=base_url, token=self.token)

        # A GitLab-illegal separator for flattening path segments
        self.sep: str = kwargs.get("sep", "::")

        # Mapping tables
        self._repo_by_flat: dict[str, dict] = {}     # flat -> {id, original_path, readable_path}
        self._flat_by_original: dict[str, str] = {}  # original_path -> flat
        self._flat_by_id: dict[int, str] = {}        # id -> flat

    # ─────────────────────────────── helpers ────────────────────────────────

    def _norm_inside(self, inside: str | None) -> str:
        """
        Normalize an 'inside-repo' path for cache keys and API calls:
        - "", ".", "./"  → ""
        - "a/./b"        → "a/b"
        - removes redundant slashes; keeps relative posix semantics
        """
        if not inside:
            return ""
        norm = str(PurePosixPath(inside))
        return "" if norm == "." else norm

    def _flatten_repo(self, original_path: str) -> str:
        """
        Convert a GitLab project path to a flattened, filesystem-safe key.
        NOTE: Only call this for projects / repositories, not subpaths!

        The function replaces all "/" in ``original_path`` with ``self.sep`` to
        produce a single-segment name used in the flattened namespace. 

        Args:
            original_path: The canonical GitLab path of the project
                (e.g., "group/subgroup/repo").

        Returns:
            A flattened repository key (string) suitable for use as a top-level
            directory name in the virtual filesystem. 

        """
        return original_path.replace("/", self.sep)


    def _register_repo(self, *, project_id: int, original_path: str, readable_path="") -> str:
        """
        Register a project in flat-name maps, returning its flat key.

        Args:
            project_id (int): Numeric GitLab project ID.
            original_path (str): Canonical GitLab path (e.g., "group/sub/repo").
            readable_path (str): Human-friendly path to show in UIs. (Optional)

        Returns:
            str: The flattened repository key added to the mappings.
        """
        flat = self._flatten_repo(original_path)
        existing = self._repo_by_flat.get(flat)
        if existing:
            # invariant: same flat must map to same id
            if existing["id"] != project_id:
                raise ValueError(f"flat '{flat}' already bound to id {existing['id']}, got {project_id}")
            return flat  # idempotent: maps already correct

        # first registration: update maps only
        self._repo_by_flat[flat] = {
            "id": project_id,
            "original_path": original_path,
            "readable_path": readable_path,
        }
        self._flat_by_original[original_path] = flat
        self._flat_by_id[project_id] = flat
        return flat

    async def _ensure_repo_registered_flat(self, flat_key: str) -> dict:
        # already known
        repo = self._repo_by_flat.get(flat_key)
        if repo:
            return {**repo, "flat": flat_key}

        # reconstruct canonical path and resolve by path
        original = flat_key.replace(self.sep, "/")
        proj = await self.gitlab_client.get_project_by_path(original)
        if proj is None:
            raise FileNotFoundError(
                f"Unknown repository key '{flat_key}'. Use canonical 'group/sub/repo' "
                f"or provide the correct flat key using '{self.sep}'."
            )
        flat = self._register_repo( 
            project_id=proj["id"],
            original_path=proj["original_path"],
            readable_path=proj["readable_path"],
        )
        return {**self._repo_by_flat[flat], "flat": flat}
    
    async def _update_root_dir(self, *, refresh: bool = False):
        if refresh or "" not in self.dircache:
            # replace root list on refresh, or build if missing
            self.dircache[""] = []
            projects = await self.gitlab_client.retrieve_root_level()
            for entry in projects:
                flat = self._register_repo(
                    project_id=entry["id"],
                    original_path=entry["original_path"],
                    readable_path=entry["readable_path"],
                )
                self.dircache[""].append({
                    "name": flat,
                    "type": "dir",
                    "size": None,
                    "is_project": True,
                })


    async def _update_project_dir(self, flat_key: str, repo_id: int, subdir: str):
        """
        Populate dircache entries for a repository subdirectory.

        Notes
        -----
        - `subdir` is an inside-repo path; "" means repo root.
        - GitLab `repository/tree` returns entries with `path` **repo-root-relative**.
          Therefore we must not re-prefix `subdir` when composing cache keys, or
          we will duplicate it (e.g. `<repo>/dir/dir/file`).
        - Parent cache keys are built from the **repo-root-relative** parent path
          and normalized so that `"."` → "".
        """
        subdir = self._norm_inside(subdir)
        files = await self.gitlab_client.retrieve_project_level(repo_id, self.per_page, subdir)

        # Ensure the directory listings exists in cache
        listing_key = flat_key if not subdir else f"{flat_key}/{subdir}"
        self.dircache.setdefault(listing_key, [])

        for entry in files:
            # GitLab returns repo-root-relative paths
            rel = entry["path"]
            ## (Safety) if API ever returns subdir-relative, re-root it
            #if subdir and not rel.startswith(subdir + "/") and rel != subdir:
            #    rel = f"{subdir}/{rel}"

            # Parent inside the repo; normalize so '.' → ''
            parent_rel = self._norm_inside(str(PurePosixPath(rel).parent))
            parent_cache = flat_key if not parent_rel else f"{flat_key}/{parent_rel}"
            self.dircache.setdefault(parent_cache, [])

            # If this is a directory, ensure its own cache key exists
            if entry["type"] == "tree":
                self.dircache.setdefault(f"{flat_key}/{rel}", [])

            # Append child entry to its parent listing
            val = {
                "name": entry["name"],
                "type": "dir" if entry["type"] == "tree" else "file",
                "size": None,
            }
            # Check for duplicates
            if not any(e.get("name") == val["name"] and e.get("type") == val["type"] for e in self.dircache[parent_cache]):
                self.dircache[parent_cache].append(val)

    async def _resolve_path(self, path: str) -> tuple[dict, str]:
        """
        Resolve any input path into (repo_record, inside_path).

        - Flat addressing: first component contains `self.sep` or id suffix `__<digits>`.
        - Canonical addressing: otherwise, try decreasing prefixes against GET /projects/:path.
        """
        clean = (path or "").lstrip("/")
        if not clean:
            return ({}, "")

        first = clean.split("/", 1)[0]
        is_flat = (self.sep in first)

        if is_flat:
            head, tail = map(str, split_first(clean))
            repo = await self._ensure_repo_registered_flat(head)
            return repo, self._norm_inside(tail)

        # canonical paths: try decreasing prefixes 
        # (FIRST: check local maps to avoid HTTP)
        parts = clean.split("/")
        for i in range(len(parts), 0, -1):
            candidate = "/".join(parts[:i])
            flat = self._flat_by_original.get(candidate)
            if flat:
                repo = {**self._repo_by_flat[flat], "flat": flat}
                remainder = "/".join(parts[i:])
                return repo, self._norm_inside(remainder)

        # fallback: do the HTTP lookups only if not known locally
        for i in range(len(parts), 0, -1):
            candidate = "/".join(parts[:i])
            proj = await self.gitlab_client.get_project_by_path(candidate)
            if proj is not None:
                flat = self._register_repo(
                    project_id=proj["id"],
                    original_path=proj["original_path"],
                    readable_path=proj["readable_path"],
                )
                repo = {**self._repo_by_flat[flat], "flat": flat}
                remainder = "/".join(parts[i:])
                return repo, self._norm_inside(remainder)

        raise FileNotFoundError(f"No matching project found in path '{path}'")
    
    ##### Async replacement if fallbnack takes to much time. But should be rarely needed ####
    """
    candidates: list[tuple[int, str]] = [(i, "/".join(parts[:i])) for i in range(len(parts), 0, -1)]

    async def fetch(i: int, cand: str):
        async with self.semaphore:
            try:
                proj = await self.gitlab_client.get_project_by_path(cand)
            except Exception:
                proj = None
            return i, cand, proj

    results = await asyncio.gather(*(fetch(i, cand) for i, cand in candidates))

    # `gather` preserves input order (longest→shortest), so the first non-None is the longest match
    for i, cand, proj in results:
        if proj is not None:
            flat = self._register_repo(
                project_id=proj["id"],
                original_path=proj["original_path"],
                readable_path=proj["readable_path"],
            )
            repo = {**self._repo_by_flat[flat], "flat": flat}
            remainder = "/".join(parts[i:])
            return repo, self._norm_inside(remainder)
    """


    # ───────────────────────────── fsspec methods ────────────────────────────
    async def close(self):
        await self.gitlab_client.close()
    
    async def _ls(self, path, detail=True, **kwargs):
        refresh = kwargs.get("refresh", False)
        path = (path or "").strip("/")

        if not path:
            if refresh or "" not in self.dircache:
                await self._update_root_dir()
            listing = self.dircache.get("", [])
            return listing if detail else [d["name"] for d in listing]

        repo, inside = await self._resolve_path(path)      # inside already normalized
        key = repo["flat"]
        cache_key = f"{key}/{inside}" if inside else key

        if refresh or cache_key not in self.dircache:
            await self._update_project_dir(key, repo["id"], inside)  # pass normalized subdir

        listing = self.dircache.get(cache_key, [])
        return listing if detail else [d["name"] for d in listing]

    async def _info(self, path, **kwargs):
        path = (path or "").strip("/")
        # If path is the empty string, return root info.
        if not path:
            if "" not in self.dircache:
                await self._update_root_dir()
            return self.dircache[""]

        repo, inside = await self._resolve_path(path)   # normalized
        key = repo["flat"]

        if not inside:
            return {
                "name": Path(key).name,
                "type": "directory",
                "size": None,
                "path": key,
                "is_project": True,
            }

        parent_rel = self._norm_inside(str(PurePosixPath(inside).parent))
        parent_cache = f"{key}/{parent_rel}" if parent_rel else key
        if parent_cache not in self.dircache:
            await self._update_project_dir(key, repo["id"], parent_rel)

        name = Path(inside).name
        for item in self.dircache.get(parent_cache, []):
            if item["name"] == name:
                return {
                    "name": item["name"],
                    "type": item["type"],
                    "size": item.get("size"),
                    "path": f"{key}/{inside}",
                    "is_project": False,
                }
        raise FileNotFoundError(f"Path '{path}' not found in repository.")


    async def _open(self, path, mode='rb', block_size=None, autocommit=True,
                    cache_options=None, compression=None, **kwargs) -> AbstractAsyncStreamedFile:
        is_write = 'w' in mode or 'a' in mode
        is_read = 'r' in mode

        repo, inside = await self._resolve_path(path)   # normalized
        if not inside:
            raise IsADirectoryError(f"'{path}' points to a repository, not a file")

        key = repo["flat"]
        parent_rel = self._norm_inside(str(PurePosixPath(inside).parent))
        parent_cache = f"{key}/{parent_rel}" if parent_rel else key
        if parent_cache not in self.dircache:
            await self._update_project_dir(key, repo["id"], parent_rel)

        name = Path(inside).name
        exists = any(e["name"] == name for e in self.dircache.get(parent_cache, []))

        if is_write:
            if exists:
                raise FileExistsError(f"{path} already exists — refusing to overwrite with LFS.")
            return AsyncLFSFile(
                fs=self,
                path=inside,                 # normalized
                token=self.token,
                host=self._base_url,
                namespace=repo["original_path"],
                repo_id=repo["id"],
                ref="main",
                mode=mode,
            )
        elif is_read:
            if not exists:
                await self._update_project_dir(key, repo["id"], parent_rel)
                parent_listing = self.dircache.get(parent_cache, [])
                if not any(e["name"] == name for e in parent_listing):
                    raise FileNotFoundError(path)
            return AsyncLFSFile(
                fs=self,
                path=inside,                 # normalized
                token=self.token,
                host=self._base_url,
                namespace=repo["original_path"],
                repo_id=repo["id"],
                ref="main",
                mode=mode,
            )
        else:
            raise NotImplementedError(f"Unsupported file mode: {mode}")


if __name__ == "__main__":

    async def main():
        load_dotenv()
        token = os.getenv("GITLAB_TOKEN")
        base_url = "https://git.nfdi4plants.org"
        teschd = GitLab_ARC_FileSystem(base_url=base_url, token=token)


        # Examples of the new capabilities:
        # 1) Canonical path listing without listing root first
        # await fs._ls("group/sub/repo", detail=True)

        # 2) Mixed addressing: canonical to open, flattened to continue
        # f = await fs._open("group/sub/repo/path/to/file.txt", mode="rb")
        # async with f as fh:
        #     data = await fh.read()

        

        # result = await teschd._retrieve_root_level(per_page=100)
        # res = await teschd._retrieve_project_level("usadellab-Barvista_ARC")
        #res = await teschd._ls("", detail=False)
        #res = await teschd._ls("usadellab-Barvista_ARC", detail=False)
        #print(res)
        # print(res)
        #await teschd._get_file("usadellab-Barvista_ARC/README.md", "README.md")
        print(await teschd._exists("usadellab::Barvista_ARC/README.md"))
        print(await teschd._exists("usadellab Barvista_ARC/README."))
        print(await teschd._isdir("usadellab-Barvista_ARC"))
        print(await teschd._isfile("usadellab-Barvista_ARC/README.md"))
        print(await teschd._info("usadellab-Barvista_ARC/README.md"))

        bala = await teschd._ls("julian.weidhase::test12345", detail=True)
        print(bala)

        local_path = "Unbenannt.jpeg"
        remote_path = "julian.weidhase::test12345/Unbenannt.jpeg"

        # Read local file data
        with open(local_path, "rb") as f_local:
            data = f_local.read()

        # Async open and write
        f_remote = await teschd._open(remote_path, mode="wb")
        async with f_remote:  # This works because AsyncLFSFile implements __aenter__ and __aexit__
            await f_remote.write(data)

        await teschd.close()


    asyncio.run(main())
