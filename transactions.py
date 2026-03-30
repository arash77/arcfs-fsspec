from __future__ import annotations

import base64
import hashlib
from pathlib import PurePosixPath
from typing import Any, Optional

from utils import legacy_gitattributes_block, lfs_pointer_text


async def commit_pointer(*, client, repo_id: int, branch: str, filepath: str, sha: str, size: int) -> None:
    pointer = lfs_pointer_text(sha, size)
    actions = [{"action": "create", "file_path": filepath, "content": pointer, "encoding": "text"}]
    await client.create_commit(repo_id, branch, f"Add LFS pointer {filepath}", actions)


async def move_pointer(*, client, repo_id: int, branch: str, src: str, dst: str) -> None:
    actions = [{"action": "move", "file_path": dst, "previous_path": src}]
    await client.create_commit(repo_id, branch, f"Move pointer {src} -> {dst}", actions)


async def update_gitattributes_legacy(*, client, repo_id: int, branch: str, path_str: str) -> None:
    ga_path = ".gitattributes"
    block = legacy_gitattributes_block(path_str)

    try:
        file_json = await client.get_file(repo_id, ga_path, branch)
        existing = base64.b64decode(file_json["content"]).decode("utf-8", errors="replace")
        updated = (existing + "\n" + block) if existing else block
        actions = [{"action": "update", "file_path": ga_path, "content": updated, "encoding": "text"}]
        await client.create_commit(repo_id, branch, f"Update {ga_path}", actions)
    except FileNotFoundError:
        actions = [{"action": "create", "file_path": ga_path, "content": block, "encoding": "text"}]
        await client.create_commit(repo_id, branch, f"Add {ga_path}", actions)


async def default_branch(*, client, repo_id: int) -> str:
    proj = await client.get_project(repo_id)
    return proj.get("default_branch") or "main"


async def commit_lfs_transaction(
    *,
    client,
    host: str,
    token: str,
    repo: dict[str, Any],
    base_branch: Optional[str],
    final_path: str,
    sha: str,
    size: int,
    data_stream,
    feature_branch_prefix: str = "run_results",
    tmp_pointer_name: bool = True,
    create_mr: bool = True,
) -> str:
    """Perform the full LFS-safe upload+commit workflow.

    Args:
        repo: must include at least {"id": int, "original_path": "group/sub/repo"}

    Returns:
        The created feature branch name.
    """
    repo_id = repo["id"]
    namespace = repo["original_path"]  # project path_with_namespace
    base = base_branch or await default_branch(client=client, repo_id=repo_id)

    token_sha = hashlib.sha256(str(token).encode("utf-8")).hexdigest()
    feature = f"{feature_branch_prefix}-{token_sha}"

    # Create branch; if it exists already, continue on it.
    await client.create_branch(repo_id, feature, base)

    payload = {
        "operation": "upload",
        "objects": [{"oid": sha, "size": size}],
        "transfers": ["basic"],
        "ref": {"name": f"refs/heads/{base}"},
    }
    batch_resp = await client.lfs_batch(host, namespace, token, payload)
    obj0 = batch_resp["objects"][0]
    upload = (obj0.get("actions") or {}).get("upload")

    # Upload the binary only if the server instructs us to.
    if upload:
        await client.lfs_upload(token, upload["href"], upload.get("header") or {}, data_stream)

    # Commit pointer(s) in the feature branch.
    p = PurePosixPath(final_path)
    if tmp_pointer_name:
        # Legacy trick: create pointer as <sha> then move to final name.
        tmp_path = str(p.parent / sha) if str(p.parent) not in ("", ".") else sha
        await commit_pointer(client=client, repo_id=repo_id, branch=feature, filepath=tmp_path, sha=sha, size=size)
        await update_gitattributes_legacy(client=client, repo_id=repo_id, branch=feature, path_str=str(p))
        await move_pointer(client=client, repo_id=repo_id, branch=feature, src=tmp_path, dst=str(p))
    else:
        await commit_pointer(client=client, repo_id=repo_id, branch=feature, filepath=str(p), sha=sha, size=size)
        await update_gitattributes_legacy(client=client, repo_id=repo_id, branch=feature, path_str=str(p))

    if create_mr:
        await client.create_merge_request(
            repo_id=repo_id,
            source_branch=feature,
            target_branch=base,
            title=f"LFS upload {final_path}",
        )

    return feature
