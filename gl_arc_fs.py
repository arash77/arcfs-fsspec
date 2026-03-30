from __future__ import annotations

from typing import Optional

import fsspec
from fsspec.asyn import AsyncFileSystem
from gitlab_client import GitLabClient
from utils import norm_inside


class GitLabARCFileSystem(AsyncFileSystem):
    """
    fsspec filesystem for GitLab repositories.

    Path forms supported:
      - Raw GitLab paths:
            group/subgroup/repo/path/to/file
      - Marker paths (internal / canonical):
            group/subgroup:-:repo/path/to/file

    Internally, everything resolves to:
        (repo_id, inside_path)
    """

    root_marker = ":-:"

    def __init__(
        self,
        base_url: str,
        token: str | None,
        asynchronous: bool = False,
        **kwargs,
    ):
        super().__init__(asynchronous=asynchronous, **kwargs)

        self.client = GitLabClient(base_url, token)

        # Project cache:
        #   original_path -> {"id": int, "original_path": str}
        self.repos: dict[str, dict] = {}

        # Negative cache for failed raw-prefix probes
        self.not_repo: set[str] = set()

        # fsspec directory cache
        self.dircache: dict[str, list[dict]] = {}

        # Root index state
        self._project_index_built: bool = False
        self._project_index_building: bool = False

    async def _ensure_project_index(self, *, refresh: bool = False, concurrent_offset: bool = False) -> None:
        """
        Ensure the in-memory GitLab project index is available.

        Builds an index of accessible projects keyed by ``original_path`` the first
        time it is needed. Subsequent calls are no-ops unless ``refresh=True`` is
        given.

        Args:
            refresh: If True, rebuild and replace the cached project index even if it
                already exists.
            concurrent_offset: If True, allow the client to fetch root project pages
                concurrently when supported.

        Side effects:
            - Populates or replaces ``self.repos``.
            - Clears ``self.not_repo`` after a successful rebuild.
            - Sets internal flags to prevent concurrent rebuilds.

        Returns:
            None.
        """
        if self._project_index_built and not refresh:
            return

        if self._project_index_building:
            while self._project_index_building:
                await fsspec.asyn.asyncio.sleep(0.01)
            return

        self._project_index_building = True
        try:
            projects = await self.client.retrieve_root_level(
                per_page=100,
                simple=True,
                concurrent_offset=concurrent_offset,
            )
            self.repos.clear()
            self.repos.update({p["original_path"]: p for p in projects})
            if refresh:
                self.not_repo.clear()
            self._project_index_built = True
        finally:
            self._project_index_building = False

    # ------------------------------------------------------------------
    # Resolver helpers
    # ------------------------------------------------------------------
    def _resolve_from_cache(self, raw_path: str) -> Optional[tuple[dict, str]]:
        """
        Resolve a raw GitLab path using only the local repo cache.

        Uses longest-prefix matching against ``self.repos`` keys. E.G. if
        ``self.repos`` contains "group/sub/repo" and ``raw_path`` is
        "group/sub/repo/path/to/file", this returns (repo, "path/to/file").

        Returns:
            (repo, inside_path) if a cached repo prefix is found, otherwise None.
        """
        parts = [p for p in raw_path.strip("/").split("/") if p]
        if not parts:
            return None

        for i in range(len(parts), 0, -1):
            candidate = "/".join(parts[:i])
            repo = self.repos.get(candidate)
            if repo:
                inside = "/".join(parts[i:])
                return repo, norm_inside(inside)
        return None

    async def _resolve_marker(self, path: str) -> tuple[dict, str]:
        """
        Resolve a marker path: namespace:-:repo[/inside]
        """
        head, rest = path.split(self.root_marker, 1)
        namespace = head.strip("/")
        rest = rest.lstrip("/")

        repo_name = rest.split("/", 1)[0]
        inside = rest[len(repo_name):].lstrip("/")

        original_path = (
            f"{namespace}/{repo_name}" if namespace else repo_name
        )

        repo = self.repos.get(original_path)
        if not repo:
            repo = await self.client.get_project_by_path(original_path)
            if not repo:
                raise FileNotFoundError(original_path)
            self.repos[repo["original_path"]] = repo

        return repo, norm_inside(inside)

    async def _resolve_raw(self, path: str, *,refresh: bool = False, **kwargs ) -> tuple[dict, str]:
        concurrent_offset = bool(kwargs.get("concurrent_offset", False))

        # 1) cache-only lookup (longest prefix)
        hit = self._resolve_from_cache(path)
        if hit is not None:
            return hit

        # 2) DIRECT lookup: the whole path might be the repo root
        proj = await self.client.get_project_by_path(path)
        if proj:
            self.repos[proj["original_path"]] = proj
            return proj, ""  # repo root = directory

        # 3) lazy probing for prefixes (namespace/project/inside/...)
        parts = [p for p in path.strip("/").split("/") if p]
        for i in range(len(parts), 0, -1):
            candidate = "/".join(parts[:i])
            if candidate in self.not_repo:
                continue

            proj = await self.client.get_project_by_path(candidate)
            if proj:
                self.repos[proj["original_path"]] = proj
                inside = "/".join(parts[i:])
                return proj, norm_inside(inside)

            self.not_repo.add(candidate)

        # 4) build full index once, retry
        await self._ensure_project_index(refresh=refresh, concurrent_offset=concurrent_offset)

        hit = self._resolve_from_cache(path)
        if hit is not None:
            return hit

        raise FileNotFoundError(path)

    async def _resolve(
        self,
        path: str,
        *,
        refresh: bool = False,
        **kwargs
    ) -> tuple[dict, str]:
        """
        Resolve any filesystem path into (repo, inside_path).
        """
        concurrent_offset = bool(kwargs.get("concurrent_offset", False))
        path = (path or "").strip().strip("/")
        if self.root_marker in path:
            return await self._resolve_marker(path)
        return await self._resolve_raw(path, refresh=refresh, **kwargs)

    # ------------------------------------------------------------------
    # fsspec API
    # ------------------------------------------------------------------
    async def _ls(self, path: str, detail: bool = True, **kwargs):
        """
        List a directory.

        refresh=True forces cache invalidation and index rebuild.

        Paginated listings are fetched directly from the backend and are not served from the normal directory cache.
        """
        refresh = bool(kwargs.get("refresh", False))
        concurrent_offset = bool(kwargs.get("concurrent_offset", False))
        paginate = bool(kwargs.get("paginate", False))
        page = kwargs.get("page")
        per_page = kwargs.get("per_page")

        if concurrent_offset and paginate:
            raise ValueError("paginate cannot be used together with concurrent_offset")

        if paginate and (page is None or per_page is None):
            raise ValueError("paginate=True requires both page and per_page to be set")

        if page is not None:
            page = int(page)
        if per_page is not None:
            per_page = int(per_page)

        if paginate and page < 1:
            raise ValueError("page must be >= 1")
        if paginate and per_page < 1:
            raise ValueError("per_page must be >= 1")

        path = (path or "").strip().strip("/")

        if path == "":
            if paginate:
                await self.client.retrieve_root_level()

            cache_key = "__root__"
            if refresh or cache_key not in self.dircache:
                await self._ensure_project_index(refresh=refresh, concurrent_offset=concurrent_offset)
                self.dircache[cache_key] = [
                    {
                        "name": f"{repo['original_path']}{self.root_marker}",
                        "type": "directory",
                    }
                    for repo in self.repos.values()
                ]

            out = self.dircache[cache_key]
            return out if detail else [e["name"] for e in out]

        repo, inside = await self._resolve(path, refresh=refresh, concurrent_offset=concurrent_offset)
        key = f"{repo['original_path']}{self.root_marker}"
        cache_key = f"{key}/{inside}" if inside else key

        if refresh or cache_key not in self.dircache:
            items = await self.client.retrieve_project_level(
                repo["id"],
                inside,
                ref=kwargs.get("ref") or "main",
            )
            self.dircache[cache_key] = [
                {
                    "name": f"{key}{i['path']}",
                    "type": "directory" if i.get("type") == "tree" else "file",
                }
                for i in items
            ]

        out = self.dircache[cache_key]
        return out if detail else [e["name"] for e in out]

    async def _close(self):
        await self.client.close()

    # ------------------------------------------------------------------
    # Explicitly disabled destructive operations
    # ------------------------------------------------------------------
    async def _rm_file(self, path, **kwargs):
        raise PermissionError("Delete operations are disabled.")

    async def _rm(self, path, recursive=False, batch_size=None, **kwargs):
        raise PermissionError("Delete operations are disabled.")
