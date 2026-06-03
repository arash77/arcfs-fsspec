from __future__ import annotations

import base64
import hashlib
from pathlib import PurePosixPath
from typing import Any, Optional

from .utils import gitattributes_block, lfs_pointer_text


async def commit_pointer(*, client, repo_id: int, branch: str, filepath: str, sha: str, size: int) -> None:
    """
    Commit a Git LFS pointer file to a repository branch.

    Args:
        client: GitLab client exposing ``create_commit``.
        repo_id: Numeric GitLab project id.
        branch: Branch that receives the pointer commit.
        filepath: Repository-internal pointer file path.
        sha: SHA256 object id for the LFS object.
        size: Size of the LFS object in bytes.

    Returns:
        None.
    """
    pointer = lfs_pointer_text(sha, size)
    actions = [{"action": "create", "file_path": filepath, "content": pointer, "encoding": "text"}]
    await client.create_commit(repo_id, branch, f"Add LFS pointer {filepath}", actions)


async def move_pointer(*, client, repo_id: int, branch: str, src: str, dst: str) -> None:
    """
    Move an existing pointer file to its final repository path.

    Args:
        client: GitLab client exposing ``create_commit``.
        repo_id: Numeric GitLab project id.
        branch: Branch that receives the move commit.
        src: Current repository-internal pointer path.
        dst: Final repository-internal pointer path.

    Returns:
        None.
    """
    actions = [{"action": "move", "file_path": dst, "previous_path": src}]
    await client.create_commit(repo_id, branch, f"Move pointer {src} -> {dst}", actions)


async def update_gitattributes(*, client, repo_id: int, branch: str, path_str: str) -> None:
    """
    Create or update ``.gitattributes`` so a path is tracked by Git LFS.

    Args:
        client: GitLab client exposing ``get_file`` and ``create_commit``.
        repo_id: Numeric GitLab project id.
        branch: Branch that receives the ``.gitattributes`` commit.
        path_str: Repository-internal path that should be LFS-tracked.

    Returns:
        None.
    """
    ga_path = ".gitattributes"
    block = gitattributes_block(path_str)

    try:
        file_json = await client.get_file(repo_id, ga_path, branch)
        existing = base64.b64decode(file_json["content"]).decode("utf-8", errors="replace")
        updated = (existing + "\n" + block) if existing else block
        actions = [{"action": "update", "file_path": ga_path, "content": updated, "encoding": "text"}]
        await client.create_commit(repo_id, branch, f"Update {ga_path}", actions)
    except FileNotFoundError:
        actions = [{"action": "create", "file_path": ga_path, "content": block, "encoding": "text"}]
        await client.create_commit(repo_id, branch, f"Add {ga_path}", actions)


async def commit_lfs_transaction(
    *,
    client,
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
    """
    Perform the full LFS upload and pointer-commit workflow.

    Args:
        client: GitLab client exposing branch, commit, merge request, and LFS
            helpers.
        token: Token used for Git LFS upload authentication.
        repo: Project dict containing at least ``id`` and ``original_path``.
        base_branch: Branch to base the feature branch on. If ``None``, the
            project default branch is used.
        final_path: Repository-internal destination path for the LFS pointer.
        sha: SHA256 object id for the LFS object.
        size: Size of the LFS object in bytes.
        data_stream: Async readable binary stream containing the LFS object.
        feature_branch_prefix: Prefix for the generated feature branch name.
        tmp_pointer_name: If True, commit the pointer under the SHA first and
            move it to the final path after updating ``.gitattributes``.
        create_mr: If True, create a merge request after committing.

    Returns:
        Created feature branch name as ``str``.
    """
    repo_id = repo["id"]
    namespace = repo["original_path"]  # project path_with_namespace
    base = base_branch or await client.get_default_branch(repo_id)

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
    batch_resp = await client.lfs_batch(namespace, token, payload)
    obj0 = batch_resp["objects"][0]
    upload = (obj0.get("actions") or {}).get("upload")

    # Upload the binary only if the server instructs to do so.
    if upload:
        await client.lfs_upload(token, upload["href"], upload.get("header") or {}, data_stream)

    # Commit pointer(s) in the feature branch.
    p = PurePosixPath(final_path)
    if tmp_pointer_name:
        # Create pointer as <sha> then move to final name to avoid tracking pointer file itself.
        tmp_path = str(p.parent / sha) if str(p.parent) not in ("", ".") else sha
        await commit_pointer(client=client, repo_id=repo_id, branch=feature, filepath=tmp_path, sha=sha, size=size)
        await update_gitattributes(client=client, repo_id=repo_id, branch=feature, path_str=str(p))
        await move_pointer(client=client, repo_id=repo_id, branch=feature, src=tmp_path, dst=str(p))
    else:
        await commit_pointer(client=client, repo_id=repo_id, branch=feature, filepath=str(p), sha=sha, size=size)
        await update_gitattributes(client=client, repo_id=repo_id, branch=feature, path_str=str(p))

    if create_mr:
        await client.create_merge_request(
            repo_id=repo_id,
            source_branch=feature,
            target_branch=base,
            title=f"LFS upload {final_path}",
        )

    return feature
