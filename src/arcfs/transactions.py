from __future__ import annotations

import base64
import hashlib
from pathlib import PurePosixPath
from typing import Any, Optional

from .errors import RefNotFound
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
    except RefNotFound:
        raise
    except FileNotFoundError:
        actions = [{"action": "create", "file_path": ga_path, "content": block, "encoding": "text"}]
        await client.create_commit(repo_id, branch, f"Add {ga_path}", actions)


async def refuse_a_directory(*, client, repo_id: int, inside: str, ref: str) -> None:
    """
    Refuse a write whose target names a directory rather than a file.

    Git stores a path as a blob or a tree and a commit may swap one for the
    other, so writing to a directory replaces that directory and everything
    under it and reports success. The files endpoint cannot catch this: it
    answers 404 for a directory exactly as it does for a path that is not
    there.

    The tree endpoint tells them apart, though not the same way on every
    version: GitLab answers a path that is not a directory with 404 from 17.7
    on and with an empty list before it, so reading the status alone would
    refuse every new file on an older self-managed instance. Git has no empty
    trees, so the entries decide it whichever way the status went.

    This lives in the transaction rather than in ``_put_file`` so that every
    write reaches it. ``open(path, "wb")`` commits through
    ``AsyncLFSFile._commit``, which calls this function's caller directly and
    would otherwise skip the check entirely.

    Args:
        client: GitLab client exposing ``retrieve_project_level_page``.
        repo_id: Numeric GitLab project id.
        inside: Repository-internal path the write targets.
        ref: Branch the write commits to.

    Returns:
        None.

    Raises:
        IsADirectoryError: If ``inside`` is a directory on ``ref``.
    """
    try:
        entries, _ = await client.retrieve_project_level_page(
            repo_id=repo_id, subdir=inside, ref=ref, page=1, per_page=1
        )
    except FileNotFoundError:
        # The client reports every 404 from this endpoint the same way, so this cannot tell a
        # path that is not a directory from a ref or project that is not there. It is safe here
        # only because of where this runs: the caller creates ``ref`` immediately before, and a
        # project that has gone missing fails the upload that follows regardless. Moving this
        # call anywhere earlier brings that ambiguity back and the check starts passing writes
        # it never managed to make.
        return
    if entries:
        raise IsADirectoryError(inside)


def feature_branch_name(token: str, prefix: str = "run_results") -> str:
    """
    Return the branch an upload with this token commits onto.

    The name is derived from the token rather than from the file, so every export
    made with one token shares a branch and sees what the earlier ones left there.
    A caller has to be able to work out that branch before the upload starts, which
    is why this is not inlined in ``commit_lfs_transaction``.

    Args:
        token: Token the upload authenticates with.
        prefix: Prefix for the generated branch name.

    Returns:
        Branch name as ``str``.
    """
    token_sha = hashlib.sha256(str(token).encode("utf-8")).hexdigest()
    return f"{prefix}-{token_sha}"


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

    feature = feature_branch_name(token, feature_branch_prefix)

    # Create branch; if it exists already, continue on it.
    await client.create_branch(repo_id, feature, base)

    # After the branch exists it carries whatever base had, so one check on it covers both a
    # directory that was already on base and one an earlier upload put on the feature branch.
    # Before the LFS upload rather than before the commit, so a refusal leaves no uploaded
    # object behind with nothing referencing it.
    await refuse_a_directory(client=client, repo_id=repo_id, inside=final_path, ref=feature)

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
