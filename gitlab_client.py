from __future__ import annotations

import asyncio
from typing import Any, Optional
from urllib.parse import quote
import warnings
import aiohttp


class GitLabClient:
    def __init__(self, base_url: str, token: Optional[str]):
        self.base_url = base_url.rstrip("/")
        self.token = token

        self._session: aiohttp.ClientSession | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    async def _ensure(self) -> aiohttp.ClientSession:
        """
        Ensure we have a ClientSession bound to the current running event loop.

        If the loop changes (common when mixing sync wrappers + asyncio.run),
        the old session must be closed and recreated, otherwise aiohttp will throw.
        """
        loop = asyncio.get_running_loop()

        if self._loop is not None and self._loop is not loop:
            await self.close()

        if self._session is None or self._session.closed:
            headers: dict[str, str] = {}
            if self.token:
                headers["PRIVATE-TOKEN"] = self.token
            self._session = aiohttp.ClientSession(headers=headers)
            self._loop = loop

        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None
        self._loop = None

    async def get_project_by_path(self, path_with_namespace: str) -> dict[str, Any] | None:
        """
        Lookup a project by its full `path_with_namespace` (e.g. "group/sub/repo").

        Returns a minimal dict:
            {"id": <int>, "original_path": <path_with_namespace>}
        """
        s = await self._ensure()
        url = f"{self.base_url}/api/v4/projects/{quote(path_with_namespace, safe='')}"
        async with s.get(url) as r:
            if r.status == 404:
                return None
            r.raise_for_status()
            j = await r.json()
            return {"id": j["id"], "original_path": j["path_with_namespace"]}

    async def _retrieve_root_level_sequential(
            self,
            *,
            per_page: int = 100,
            membership: bool = False,
            archived: bool = False,
            simple: bool = True,
    ) -> list[dict[str, Any]]:
        """
        Old behavior: sequential offset pagination via X-Next-Page.
        """
        s = await self._ensure()
        url = f"{self.base_url}/api/v4/projects"

        params: dict[str, Any] = {
            "per_page": int(per_page),
            "page": 1,
            "order_by": "last_activity_at",
            "sort": "desc",
            "membership": str(bool(membership)).lower(),
            "archived": str(bool(archived)).lower(),
        }
        if simple:
            params["simple"] = "true"

        out: list[dict[str, Any]] = []

        while True:
            async with s.get(url, params=params) as r:
                r.raise_for_status()
                data = await r.json()
                out.extend(
                    {
                        "id": p["id"],
                        "original_path": p["path_with_namespace"],
                    }
                    for p in data
                )

                next_page = r.headers.get("X-Next-Page") or ""
                if not next_page:
                    break
                params["page"] = int(next_page)

        return out

    async def retrieve_root_level(
            self,
            *,
            per_page: int = 100,
            page: int | None = None,
            paginate: bool = False,
            membership: bool = False,
            archived: bool = False,
            simple: bool = True,
            concurrent_offset: bool = False,
            max_concurrency: int = 8,
    ) -> list[dict[str, Any]]:
        """
        Public root listing via GET /projects.

        Notes:
          - This currently supports offset-based pagination only.
          - Keyset pagination is not implemented here.

        Modes:
          - paginate=False, concurrent_offset=False:
              use sequential offset pagination and return the full listing.
          - paginate=False, concurrent_offset=True:
              fetch page 1 first, then remaining offset pages concurrently
              when X-Total-Pages is available; otherwise fall back to sequential.
          - paginate=True:
              fetch exactly one offset page and return only that page.
              This mode is incompatible with concurrent_offset=True.
        """
        if per_page < 1:
            raise ValueError("per_page must be >= 1")

        if paginate:
            if page is None:
                raise ValueError("paginate=True requires page to be set")
            if page < 1:
                raise ValueError("page must be >= 1")

        if paginate and concurrent_offset:
            raise ValueError("paginate=True cannot be used together with concurrent_offset=True")

        s = await self._ensure()
        url = f"{self.base_url}/api/v4/projects"

        base_params: dict[str, Any] = {
            "per_page": int(per_page),
            "order_by": "last_activity_at",
            "sort": "desc",
            "membership": str(bool(membership)).lower(),
            "archived": str(bool(archived)).lower(),
        }
        if simple:
            base_params["simple"] = "true"

        def normalize(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
            return [
                {
                    "id": p["id"],
                    "original_path": p["path_with_namespace"],
                }
                for p in data
            ]

        def warn_fallback(reason: str) -> None:
            warnings.warn(
                f"retrieve_root_level: falling back to sequential pagination ({reason})",
                RuntimeWarning,
                stacklevel=2,
            )

        async def fetch_page(page_num: int) -> tuple[int, list[dict[str, Any]], dict[str, str]]:
            params = dict(base_params)
            params["page"] = page_num
            async with s.get(url, params=params) as r:
                r.raise_for_status()
                data = await r.json()
                headers = {k.lower(): v for k, v in r.headers.items()}
                return page_num, normalize(data), headers

        # Paged mode: fetch exactly one page
        if paginate:
            _, items, _ = await fetch_page(page)
            return items

        # Full-list mode: existing sequential behavior
        if not concurrent_offset:
            return await self._retrieve_root_level_sequential(
                per_page=per_page,
                membership=membership,
                archived=archived,
                simple=simple,
            )

        # Full-list mode with concurrent offset fetching
        try:
            _, first_items, first_headers = await fetch_page(1)

            total_pages_raw = first_headers.get("x-total-pages") or ""
            if not total_pages_raw:
                warn_fallback("X-Total-Pages header unavailable")
                return await self._retrieve_root_level_sequential(
                    per_page=per_page,
                    membership=membership,
                    archived=archived,
                    simple=simple,
                )

            total_pages = int(total_pages_raw)
            if total_pages <= 1:
                return first_items

            semaphore = asyncio.Semaphore(max(1, int(max_concurrency)))

            async def bounded_fetch(page_num: int) -> tuple[int, list[dict[str, Any]]]:
                async with semaphore:
                    fetched_page, items_b, _ = await fetch_page(page_num)
                    return fetched_page, items_b

            results = await asyncio.gather(
                *(bounded_fetch(page_num) for page_num in range(2, total_pages + 1))
            )

            results.sort(key=lambda x: x[0])

            out = list(first_items)
            for _, items in results:
                out.extend(items)

            return out

        except Exception as exc:
            warn_fallback(f"concurrent fetch failed: {exc!r}")
            return await self._retrieve_root_level_sequential(
                per_page=per_page,
                membership=membership,
                archived=archived,
                simple=simple,
            )

    async def retrieve_project_level(
        self,
        repo_id: int,
        subdir: str,
        *,
        ref: str = "main",
        per_page: int = 100,
    ) -> list[dict[str, Any]]:
        """
        List a directory in a repository: GET /projects/:id/repository/tree (paged).

        Returns the raw JSON items (each has "type" in {"blob","tree"} and "path"/"name").
        """
        s = await self._ensure()
        url = f"{self.base_url}/api/v4/projects/{repo_id}/repository/tree"

        params: dict[str, Any] = {"ref": ref, "per_page": int(per_page), "page": 1}
        if subdir:
            params["path"] = subdir

        out: list[dict[str, Any]] = []

        while True:
            async with s.get(url, params=params) as r:
                r.raise_for_status()
                out.extend(await r.json())

                next_page = r.headers.get("X-Next-Page") or ""
                if not next_page:
                    break
                params["page"] = int(next_page)

        return out

    async def get_raw_file(self, repo_id: int, path: str, ref: str) -> bytes:
        """
        Fetch raw file content. We pass lfs=true so GitLab resolves LFS objects.
        """
        s = await self._ensure()
        url = f"{self.base_url}/api/v4/projects/{repo_id}/repository/files/{quote(path, safe='')}/raw"

        async with s.get(url, params={"ref": ref, "lfs": "true"}) as r:
            if r.status == 404:
                raise FileNotFoundError(path)
            r.raise_for_status()
            return await r.read()
