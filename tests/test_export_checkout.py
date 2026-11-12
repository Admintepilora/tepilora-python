"""Offline regression for the tag ref overwritten during the old checkout path."""

import subprocess

import pytest

pytest.importorskip("tomllib")

from scripts import readiness_tree as tree


PAYLOAD = b'{"fixture":"annotated-export"}\n'


def git(repo, *args):
    return subprocess.check_output(["git", "-C", str(repo), *args], stderr=subprocess.PIPE)


@pytest.fixture(params=["test-v1.2.3rc2", "v1.2.3"])
def checkout(tmp_path, request):
    origin, runner = tmp_path / "origin", tmp_path / "runner"
    for repo in (origin, runner):
        repo.mkdir()
        git(repo, "init", "-q")
        git(repo, "config", "user.name", "Fixture")
        git(repo, "config", "user.email", "fixture@example.invalid")
    git(origin, "commit", "--allow-empty", "-qm", "fixture")
    commit = git(origin, "rev-parse", "HEAD").decode().strip()
    tag = request.param
    git(origin, "tag", "-a", "--cleanup=verbatim", "-m", PAYLOAD.decode(), tag)
    git(runner, "remote", "add", "origin", str(origin))
    git(runner, "fetch", "origin", "+refs/heads/*:refs/remotes/origin/*", "+refs/tags/*:refs/tags/*")
    assert git(runner, "cat-file", "-t", "refs/tags/" + tag) == b"tag\n"
    return origin, runner, commit, tag


def test_commit_checkout_keeps_original_tag_object_commit_and_annotation(checkout):
    origin, runner, commit, tag = checkout
    git(runner, "checkout", "--detach", commit)
    assert git(runner, "rev-parse", "HEAD").decode().strip() == commit
    assert git(runner, "cat-file", "-t", "refs/tags/" + tag) == b"tag\n"
    assert git(runner, "rev-parse", "refs/tags/" + tag) == git(origin, "rev-parse", "refs/tags/" + tag)
    assert tree.tag_commit(runner, tag) == commit
    assert tree.annotation(runner, tag) == PAYLOAD


def test_old_checkout_refspec_loses_annotation_and_export_guard_refuses(checkout):
    origin, runner, commit, tag = checkout
    # Exact refspec observed in the failed runner; only synthetic local repos.
    git(runner, "fetch", "--no-tags", "--prune", "--no-recurse-submodules", "origin",
        "+" + commit + ":refs/tags/" + tag)
    git(runner, "checkout", "--detach", "refs/tags/" + tag)
    assert git(runner, "cat-file", "-t", "refs/tags/" + tag) == b"commit\n"
    assert tree.tag_commit(runner, tag) == commit
    with pytest.raises(tree.ReadinessError, match="^annotated tag required$"):
        tree.annotation(runner, tag)
    assert git(origin, "cat-file", "-t", "refs/tags/" + tag) == b"tag\n"
    assert tree.annotation(origin, tag) == PAYLOAD
