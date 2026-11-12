"""Ordered-pair checks use two isolated repositories, never live mirrors/indices."""

from __future__ import annotations

import copy
import hashlib
from contextlib import contextmanager
from types import SimpleNamespace
from urllib.error import URLError

import pytest

pytest.importorskip("tomllib")

from scripts import readiness_bundle as bundle, readiness_gate as gate
from scripts import readiness_tree as tree
import test_readiness_gate as helpers


def make_pair(tmp_path, fixture_digest="6" * 64):
    repos, rc_shas, final_shas, rcs, finals = {}, {}, {}, {}, {}
    sources = helpers.SOURCES
    stable_sources = {**sources, "sdk": "8" * 40, "mcp": "9" * 40}
    for package in bundle.ORDER:
        repo = tmp_path / package
        repo.mkdir()
        helpers.git(repo, "init", "-q")
        (repo / "code.py").write_text("VALUE = 1\n")
        rc_shas[package] = helpers.commit(repo, package, "1.2.3rc1")
        repos[package] = repo
    rc_trees = {name: tree.tree_fingerprint_v1(repos[name], rc_shas[name]) for name in bundle.ORDER}
    for name, repo in repos.items():
        rc = helpers.manifest(repo, name, "1.2.3rc1", rc_shas[name])
        rc["fixture_sha256"] = fixture_digest
        rc["candidate_trees"] = rc_trees.copy()
        rc["evidence"] = {kind: helpers.report(kind, rc) for kind in ("ci", "candidate_canary")}
        helpers.tag(repo, rc)
        rcs[name] = rc
        final_shas[name] = helpers.commit(repo, name, "1.2.3", stable_sources[name])
    final_trees = {name: tree.tree_fingerprint_v1(repos[name], final_shas[name]) for name in bundle.ORDER}
    entries = {}
    for name, repo in repos.items():
        final = helpers.manifest(repo, name, "1.2.3", final_shas[name], stable_sources)
        final["fixture_sha256"] = fixture_digest
        final["candidate_trees"] = final_trees.copy()
        final["evidence"] = {kind: helpers.report(kind, final) for kind in ("ci", "candidate_canary")}
        final["evidence"]["rc_oracle"] = helpers.report("rc_oracle", final, sources=sources, trees=rc_trees)
        entries[name] = {"tag": rcs[name]["tag"], "public_sha": rc_shas[name], "private_sha": sources[name],
                         "tree_fingerprint_v1": rc_trees[name],
                         "oracle_sha256": final["evidence"]["rc_oracle"]["sha256"]}
        finals[name] = final
    common = {"version": "1.2.3", "package_index": "pypi", "release_order": bundle.ORDER.copy(),
              "policy": "version-fields-v1", "sources": stable_sources, "candidate_trees": final_trees,
              "rc": entries, "canary_sha256": finals["sdk"]["evidence"]["candidate_canary"]["sha256"],
              "partial_release": "first may publish while second is blocked; rollback by certified pin",
              "rollback": {name: finals[name]["rollback"] for name in bundle.ORDER}}
    for name, final in finals.items():
        final["bundle"] = copy.deepcopy(common)
        final["bundle_digest"] = hashlib.sha256(gate.canonical(common)).hexdigest()
        final["rc"] = {key: entries[name][key] for key in ("tag", "public_sha", "oracle_sha256")}
    return repos, final_shas, finals


@pytest.fixture
def pair(tmp_path, monkeypatch):
    repos, shas, finals = make_pair(tmp_path)

    @contextmanager
    def reader(package, tags):
        yield repos[package]

    monkeypatch.setattr(bundle, "public_mirror", reader)
    monkeypatch.setattr(bundle, "index_version_exists", lambda *args: True)
    return repos, shas, finals


def run(pair, package):
    repos, shas, finals = pair
    helpers.tag(repos[package], finals[package])
    return gate.main(["--repo", str(repos[package]), "--package", package, "--tag", "v1.2.3",
                      "--index", "pypi", "--commit", shas[package]])


def test_ordered_pair_positive_and_shared_canary(pair):
    _, _, finals = pair
    assert finals["sdk"]["bundle"] == finals["mcp"]["bundle"]
    assert finals["sdk"]["evidence"]["candidate_canary"] == finals["mcp"]["evidence"]["candidate_canary"]
    assert run(pair, "sdk") == 0
    assert run(pair, "mcp") == 0


def test_second_before_first_stable_tag_blocks(pair):
    assert run(pair, "mcp") == 1


def test_second_requires_index_existence(pair, monkeypatch):
    assert run(pair, "sdk") == 0
    monkeypatch.setattr(bundle, "index_version_exists", lambda *args: False)
    assert run(pair, "mcp") == 1


def test_first_never_claims_peer_stable_or_index_presence(pair, monkeypatch):
    monkeypatch.setattr(bundle, "index_version_exists", lambda *args: pytest.fail("first must not claim this"))
    assert run(pair, "sdk") == 0


@pytest.mark.parametrize("problem", ["reverse", "duplicate", "missing", "single", "bad_hash", "rc_sha"])
def test_invalid_order_or_bundle_blocks(pair, problem):
    final = pair[2]["sdk"]
    if problem == "reverse":
        final["release_order"] = ["mcp", "sdk"]
    elif problem == "duplicate":
        final["release_order"] = ["sdk", "sdk"]
    elif problem == "missing":
        del final["release_order"]
    elif problem == "single":
        final["release_scope"] = "sdk"
    elif problem == "bad_hash":
        final["bundle_digest"] = "0" * 64
    else:
        final["bundle"]["rc"]["mcp"]["public_sha"] = "e" * 40
        final["bundle_digest"] = hashlib.sha256(gate.canonical(final["bundle"])).hexdigest()
    assert run(pair, "sdk") == 1


def test_peer_public_mirror_unreachable_blocks(pair, monkeypatch):
    @contextmanager
    def unavailable(*args):
        raise tree.ReadinessError("fixture mirror unavailable")
        yield

    monkeypatch.setattr(bundle, "public_mirror", unavailable)
    assert run(pair, "sdk") == 1


@pytest.mark.parametrize("field", ["version", "source_sha"])
@pytest.mark.parametrize("target", ["sdk", "mcp"])
def test_current_rollback_pin_is_rejected_in_common_bundle(pair, field, target):
    finals = pair[2]
    replacement = "1.2.3" if field == "version" else finals[target]["private_source_sha"]
    for final in finals.values():
        final["bundle"]["rollback"][target][field] = replacement
        if final["package"] == target:
            final["rollback"][field] = replacement
        final["bundle_digest"] = hashlib.sha256(gate.canonical(final["bundle"])).hexdigest()
    assert run(pair, "sdk") == 1
    assert run(pair, "mcp") == 1


def test_first_from_another_bundle_blocks_second(pair):
    first = pair[2]["sdk"]
    first["bundle"]["rollback"]["mcp"]["source_sha"] = "e" * 40
    first["bundle_digest"] = hashlib.sha256(gate.canonical(first["bundle"])).hexdigest()
    assert run(pair, "sdk") == 0
    assert run(pair, "mcp") == 1


def test_peer_changed_after_canary_exposes_partial_release(pair):
    repos, shas, finals = pair
    assert run(pair, "sdk") == 0
    (repos["mcp"] / "code.py").write_text("VALUE = 2\n")
    shas["mcp"] = helpers.commit(repos["mcp"], "mcp", "1.2.3", finals["mcp"]["private_source_sha"])
    assert run(pair, "mcp") == 1


def test_public_fetch_is_anonymous_and_exact_refs(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setenv("GH_TOKEN", "not-a-real-secret")
    monkeypatch.setenv("GITHUB_TOKEN", "not-a-real-secret")
    monkeypatch.setattr(bundle.subprocess, "run", lambda command, **kw:
                        calls.append((command, kw)) or SimpleNamespace(returncode=0))
    bundle.fetch_public_refs(tmp_path, "sdk", ["test-v1.2.3rc1", "v1.2.3"])
    command, kwargs = calls[0]
    assert "GH_TOKEN" not in kwargs["env"] and "GITHUB_TOKEN" not in kwargs["env"]
    assert command[-2:] == ["refs/tags/test-v1.2.3rc1:refs/tags/test-v1.2.3rc1", "refs/tags/v1.2.3:refs/tags/v1.2.3"]
    assert "https://github.com/Admintepilora/tepilora-python.git" in command


@pytest.mark.parametrize("case", ["present", "missing", "network"])
def test_index_probe_is_public_exact_and_existence_only(monkeypatch, case):
    calls = []

    class Response:
        status = 200

        def __init__(self, url):
            self.url = url

        def geturl(self):
            return self.url

        def read(self, limit):
            return gate.canonical({"info": {"name": "Tepilora", "version": "1.2.3"},
                                   "urls": [{"filename": "fixture.whl"}] if case == "present" else []})

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    def open_request(request, timeout):
        calls.append(request)
        if case == "network":
            raise URLError("fixture unavailable")
        return Response(request.full_url)

    monkeypatch.setattr(bundle, "build_opener", lambda *args: SimpleNamespace(open=open_request))
    assert bundle.index_version_exists("Tepilora", "1.2.3", "testpypi") is (case == "present")
    assert calls[0].full_url == "https://test.pypi.org/pypi/tepilora/1.2.3/json"
    assert calls[0].get_header("Authorization") is None and calls[0].data is None
