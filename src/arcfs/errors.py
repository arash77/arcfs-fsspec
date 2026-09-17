"""Exceptions raised across arcfs modules.

Kept apart from ``gitlab_client`` so ``transactions`` can raise and catch these without the
two importing each other.
"""

from __future__ import annotations


class RefNotFound(FileNotFoundError):
    """GitLab could not resolve the ref a request named.

    Subclasses ``FileNotFoundError`` on purpose: a caller written against an earlier version
    catches that and keeps behaving as it did, and one that cares about the difference catches
    this first. That matters because the two are not always distinguishable by intent either:
    a repository with no commits answers this for its own default branch, and a caller meaning
    to write the first file there is right to treat it as "nothing to replace".
    """


__all__ = ("RefNotFound",)
