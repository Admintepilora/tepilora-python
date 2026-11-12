"""Offline fixtures only. Git tags in these tests exist only in tmp_path repos."""

from __future__ import annotations

import copy
import base64
import hashlib
import subprocess
from pathlib import Path

import pytest

pytest.importorskip("tomllib")

from scripts import readiness_gate as gate
from scripts import readiness_tree as tree


SOURCES = {"libraries": "1" * 40, "webserver": "2" * 40,
           "sdk": "3" * 40, "mcp": "4" * 40}
STAMP = "2026-01-01T00:00:00Z"


def git(repo, *args):
    return subprocess.check_output(["git", "-C", str(repo), *args], stderr=subprocess.PIPE)


def commit(repo, package, version, source=None):
    path = repo / tree.VERSION_POLICY_V1[package]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f'__version__ = "{version}"\n' if package == "sdk"
                    else f'[project]\nname = "example"\nversion = "{version}"\n', encoding="utf-8")
    git(repo, "add", "-A")
    git(repo, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
        "commit", "-m", "fixture", "-m", "Tepilora-Source-SHA: " + (source or SOURCES[package]))
    return git(repo, "rev-parse", "HEAD").decode().strip()


def report(kind, manifest, *, sources=None, fingerprint=None, trees=None):
    scopes = {manifest["package"]} if kind == "ci" else gate.SOURCES
    payload = {"schema_version": 1, "kind": kind, "provenance": gate.PROVENANCE,
               "upstream_pins": "declared release context, NOT proof of deploy",
               "status": "PASS", "exit_code": 0, "command": ["python3", "-m", "pytest"],
               "run_id": "local-fixture-1", "started_at": STAMP, "finished_at": STAMP,
               "sources": sources or manifest["sources"],
               "candidate_trees": trees or {**manifest["candidate_trees"],
                                            manifest["package"]: fingerprint or manifest["tree_fingerprint_v1"]},
               "fixture_sha256": manifest["fixture_sha256"],
               "cases": [{"id": "case-1", "status": "PASS",
                          "perimeters": dict.fromkeys(scopes, "PASS"),
                          "hashes": dict.fromkeys(scopes, "a" * 64)}]}
    return gate.inline_report(gate.canonical(payload))


def manifest(repo, package, version, commit_sha, sources=None):
    index = "testpypi" if "rc" in version else "pypi"
    payload = {"schema_version": 1, "provenance": gate.PROVENANCE,
               "producer": {"name": "northpole", "source_sha": "5" * 40},
               "package": package, "package_name": gate.PACKAGES[package],
               "version": version, "tag": ("test-v" if index == "testpypi" else "v") + version,
               "package_index": index, "private_source_sha": (sources or SOURCES)[package],
               "sources": copy.deepcopy(sources or SOURCES),
               "tree_fingerprint_v1": tree.tree_fingerprint_v1(repo, commit_sha),
               "candidate_trees": {name: tree.tree_fingerprint_v1(repo, commit_sha)
                                   if name == package else "b" * 64 for name in ("sdk", "mcp")},
               "release_scope": "both", "release_order": ["sdk", "mcp"],
               "bundle": None, "bundle_digest": None,
               "peer_verification": "bootstrap-rc" if index == "testpypi" else (
                   "peer-stable-declared-only-until-second-gate" if package == "sdk" else
                   "previous-source-validated-and-version-exists-artifact-identity-not-verified"),
               "fixture_sha256": "6" * 64, "created_at": STAMP,
               "perimeters": dict.fromkeys(gate.SOURCES, "PASS"),
               "rollback": {"version": "1.1.0", "source_sha": "7" * 40,
                            "procedure": "pin-certified-version"}, "rc": None, "evidence": {}}
    payload["evidence"] = {kind: report(kind, payload) for kind in ("ci", "candidate_canary")}
    return payload


@pytest.fixture(autouse=True)
def isolate_peer_transport_for_own_gate_tests(monkeypatch):
    """Peer transport and ordered coupling have independent, real-fixture tests."""
    monkeypatch.setattr(gate, "verify_peer", lambda manifest, **kwargs: gate.validate_bundle(manifest))


def tag(repo, payload):
    git(repo, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
        "tag", "-a", payload["tag"], "--cleanup=verbatim", "-m",
        gate.canonical(gate.seal(payload)).decode())


@pytest.fixture(params=["sdk", "mcp"])
def candidate(tmp_path, request):
    repo = tmp_path / "public"
    repo.mkdir()
    git(repo, "init", "-q")
    (repo / "code.py").write_text("VALUE = 1\n")
    package = request.param
    commit_sha = commit(repo, package, "1.2.3rc1")
    return repo, package, commit_sha, manifest(repo, package, "1.2.3rc1", commit_sha)


def test_rc_positive_and_exact_fingerprint(candidate):
    repo, package, commit_sha, payload = candidate
    expected = hashlib.sha256(git(repo, "ls-tree", "-r", "-z", "--full-tree", commit_sha)).hexdigest()
    assert payload["tree_fingerprint_v1"] == expected
    tag(repo, payload)
    assert gate.verify_tag(repo, package, payload["tag"], "testpypi", commit_sha)["version"] == "1.2.3rc1"
    assert gate.main(["--repo", str(repo), "--package", package, "--tag", payload["tag"],
                      "--index", "testpypi", "--commit", commit_sha]) == 0


@pytest.mark.parametrize("problem", ["missing_canary", "FAIL", "SKIP", "UNKNOWN", "empty_cases",
                                      "bad_report_hash", "wrong_sources", "wrong_tree", "missing_rollback",
                                      "wrong_version", "wrong_index", "wrong_private", "wrong_schema",
                                      "unsafe_command", "wrong_fixture", "hidden_failure", "future_report"])
def test_manifest_negative_cases(candidate, problem):
    repo, package, commit_sha, payload = candidate
    if problem == "missing_canary":
        del payload["evidence"]["candidate_canary"]
    elif problem == "missing_rollback":
        payload["rollback"] = None
    elif problem == "wrong_version":
        payload["version"] = "9.9.9rc1"
    elif problem == "wrong_index":
        payload["package_index"] = "pypi"
    elif problem == "wrong_private":
        payload["private_source_sha"] = "8" * 40
    elif problem == "wrong_schema":
        payload["schema_version"] = True
    else:
        wrapper = payload["evidence"]["candidate_canary"]
        raw = gate.load_json(base64.b64decode(wrapper["report_base64"]))
        if problem in {"FAIL", "SKIP", "UNKNOWN"}:
            raw["status"] = problem
        elif problem == "empty_cases":
            raw["cases"] = []
        elif problem == "bad_report_hash":
            wrapper["sha256"] = "0" * 64
        elif problem == "wrong_sources":
            raw["sources"]["libraries"] = "8" * 40
        elif problem == "wrong_tree":
            raw["candidate_trees"][package] = "8" * 64
        elif problem == "unsafe_command":
            raw["command"] = ["curl", "https://example.invalid"]
        elif problem == "wrong_fixture":
            raw["fixture_sha256"] = "8" * 64
        elif problem == "hidden_failure":
            raw["cases"].append({"id": "failed", "status": "FAIL", "perimeters": {}})
        elif problem == "future_report":
            raw["finished_at"] = "2999-01-01T00:00:00Z"
        if problem != "bad_report_hash":
            payload["evidence"]["candidate_canary"] = gate.inline_report(gate.canonical(raw))
    tag(repo, payload)
    assert gate.main(["--repo", str(repo), "--package", package, "--tag", payload["tag"],
                      "--index", "testpypi", "--commit", commit_sha]) == 1


def final_candidate(candidate):
    repo, package, rc_sha, rc = candidate
    tag(repo, rc)
    sources = {**SOURCES, package: "9" * 40}
    final_sha = commit(repo, package, "1.2.3", source=sources[package])
    final = manifest(repo, package, "1.2.3", final_sha, sources)
    final["evidence"]["rc_oracle"] = report("rc_oracle", final, sources=rc["sources"],
                                            fingerprint=rc["tree_fingerprint_v1"])
    final["rc"] = {"tag": rc["tag"], "public_sha": rc_sha,
                   "oracle_sha256": final["evidence"]["rc_oracle"]["sha256"]}
    entries = {name: {"tag": rc["tag"], "public_sha": rc_sha if name == package else "c" * 40,
                      "private_sha": rc["sources"][name],
                      "tree_fingerprint_v1": rc["candidate_trees"][name],
                      "oracle_sha256": final["evidence"]["rc_oracle"]["sha256"]}
               for name in ("sdk", "mcp")}
    final["bundle"] = {"version": final["version"], "package_index": "pypi",
                       "release_order": ["sdk", "mcp"], "policy": "version-fields-v1",
                       "sources": final["sources"], "candidate_trees": final["candidate_trees"],
                       "rc": entries, "canary_sha256": final["evidence"]["candidate_canary"]["sha256"],
                       "partial_release": "first may publish while second is blocked; rollback by certified pin",
                       "rollback": {name: final["rollback"] for name in ("sdk", "mcp")}}
    final["bundle_digest"] = hashlib.sha256(gate.canonical(final["bundle"])).hexdigest()
    return repo, package, final_sha, final


def test_release_positive_version_only(candidate):
    repo, package, final_sha, final = final_candidate(candidate)
    tag(repo, final)
    assert gate.verify_tag(repo, package, final["tag"], "pypi", final_sha)


@pytest.mark.parametrize("problem", ["rc_missing", "rc_sha", "oracle_fail", "dependency_diff",
                                      "new_file", "mode", "changelog", "extra_version_line",
                                      "cross_stack", "oracle_hash"])
def test_release_negative_cases(candidate, problem):
    repo, package, final_sha, final = final_candidate(candidate)
    if problem == "rc_missing":
        final["rc"]["tag"] = "test-v1.2.3rc9"
    elif problem == "rc_sha":
        final["rc"]["public_sha"] = final_sha
    elif problem == "oracle_fail":
        raw = gate.load_json(base64.b64decode(final["evidence"]["rc_oracle"]["report_base64"]))
        raw["status"] = "FAIL"
        final["evidence"]["rc_oracle"] = gate.inline_report(gate.canonical(raw))
        final["rc"]["oracle_sha256"] = final["evidence"]["rc_oracle"]["sha256"]
    elif problem == "oracle_hash":
        final["rc"]["oracle_sha256"] = "0" * 64
    elif problem == "cross_stack":
        final["sources"]["webserver"] = "a" * 40
        final["evidence"]["ci"] = report("ci", final)
        final["evidence"]["candidate_canary"] = report("candidate_canary", final)
    else:
        if problem == "dependency_diff":
            (repo / "code.py").write_text("VALUE = 2\n")
        elif problem == "new_file":
            (repo / "new.py").write_text("NEW = True\n")
        elif problem == "mode":
            (repo / "code.py").chmod(0o755)
        elif problem == "changelog":
            (repo / "CHANGELOG.md").write_text("1.2.3\n")
        elif problem == "extra_version_line":
            with (repo / tree.VERSION_POLICY_V1[package]).open("a") as handle:
                handle.write("# extra line\n")
        git(repo, "add", "-A")
        git(repo, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
            "commit", "-m", "unexpected", "-m", "Tepilora-Source-SHA: " + final["sources"][package])
        final_sha = git(repo, "rev-parse", "HEAD").decode().strip()
        final["tree_fingerprint_v1"] = tree.tree_fingerprint_v1(repo, final_sha)
        final["candidate_trees"][package] = final["tree_fingerprint_v1"]
        for kind in ("ci", "candidate_canary"):
            final["evidence"][kind] = report(kind, final)
        final["bundle"]["canary_sha256"] = final["evidence"]["candidate_canary"]["sha256"]
        final["bundle_digest"] = hashlib.sha256(gate.canonical(final["bundle"])).hexdigest()
    tag(repo, final)
    assert gate.main(["--repo", str(repo), "--package", package, "--tag", final["tag"],
                      "--index", "pypi", "--commit", final_sha]) == 1


def test_missing_and_lightweight_annotations_block(candidate):
    repo, package, commit_sha, payload = candidate
    args = ["--repo", str(repo), "--package", package, "--tag", payload["tag"],
            "--index", "testpypi", "--commit", commit_sha]
    assert gate.main(args) == 1
    git(repo, "tag", payload["tag"])
    assert gate.main(args) == 1


@pytest.mark.parametrize("raw", [b'{"x":1,"x":2}', b'{"x":NaN}', b'{"x":Infinity}', b'[]'])
def test_ambiguous_json_blocks(raw):
    with pytest.raises((ValueError, gate.ReadinessError)):
        gate.load_json(raw)


def test_tree_failure_and_symlink_identity(candidate):
    repo, package, commit_sha, payload = candidate
    with pytest.raises(gate.ReadinessError):
        tree.tree_fingerprint_v1(repo, "f" * 40)
    (repo / "alias.py").symlink_to("code.py")
    sha_new = commit(repo, package, "1.2.3rc1")
    assert tree.tree_fingerprint_v1(repo, sha_new) != payload["tree_fingerprint_v1"]


def test_declared_commands_are_never_executed(candidate):
    repo, package, commit_sha, payload = candidate
    raw = gate.load_json(base64.b64decode(payload["evidence"]["candidate_canary"]["report_base64"]))
    raw["command"] = ["touch", "must-not-exist"]
    payload["evidence"]["candidate_canary"] = gate.inline_report(gate.canonical(raw))
    tag(repo, payload)
    assert gate.verify_tag(repo, package, payload["tag"], "testpypi", commit_sha)
    assert not (repo / "must-not-exist").exists()


def test_repository_role_cannot_be_self_assigned(candidate):
    repo, package, commit_sha, payload = candidate
    tag(repo, payload)
    other = "tepilora-mcp-python" if package == "sdk" else "tepilora-python"
    assert gate.main(["--repo", str(repo), "--package", package, "--tag", payload["tag"],
                      "--index", "testpypi", "--commit", commit_sha,
                      "--repository", "Admintepilora/" + other]) == 1
