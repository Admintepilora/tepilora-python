"""Synthetic, offline gate CLI fixtures. Never uses a real repository or remote."""

from __future__ import annotations

import argparse
import hashlib
import io
import subprocess
import sys
import tempfile
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

try:
    from . import readiness_gate as gate, readiness_tree as tree
except ImportError:
    import readiness_gate as gate
    import readiness_tree as tree


STAMP = "2026-01-01T00:00:00Z"
SOURCES = {name: str(number) * 40 for number, name in enumerate(sorted(gate.SOURCES), 1)}
EXPECTED_BLOCKS = {
    "missing-manifest": "annotated tag required",
    "missing-canary": "required prepublication evidence missing",
    "canary-fail": "candidate_canary: report did not pass",
    "sha-mismatch": "private source mismatch",
    "version-mismatch": "version does not match tag",
    "hash-mismatch": "manifest hash mismatch",
    "rc-missing": "referenced RC tag missing or invalid",
    "oracle-fail": "rc_oracle: report did not pass",
    "diff-outside-policy": "non-version source changed since RC",
}


def git(repo: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(repo), *args], stderr=subprocess.PIPE).decode().strip()


def commit(repo: Path, package: str, version: str, source: str) -> str:
    path = repo / tree.VERSION_POLICY_V1[package]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f'__version__ = "{version}"\n' if package == "sdk"
                    else f'[project]\nname = "example"\nversion = "{version}"\n')
    git(repo, "add", "-A")
    git(repo, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit",
        "-m", "isolated fixture", "-m", "Tepilora-Source-SHA: " + source)
    return git(repo, "rev-parse", "HEAD")


def report(kind: str, payload: dict, context: dict | None = None) -> dict:
    context = context or payload
    scopes = {payload["package"]} if kind == "ci" else gate.SOURCES
    result = {"schema_version": 1, "kind": kind, "provenance": gate.PROVENANCE,
              "upstream_pins": "declared release context, NOT proof of deploy",
              "status": "PASS", "exit_code": 0, "command": ["python3", "-m", "pytest"],
              "run_id": "local-fixture", "started_at": STAMP, "finished_at": STAMP,
              "sources": context["sources"], "candidate_trees": context["candidate_trees"],
              "fixture_sha256": context["fixture_sha256"],
              "cases": [{"id": "fixture", "status": "PASS", "perimeters": dict.fromkeys(scopes, "PASS"),
                         "hashes": dict.fromkeys(scopes, "a" * 64)}]}
    return result


def manifest(repo: Path, package: str, version: str, commit_sha: str, sources: dict) -> dict:
    index = "testpypi" if "rc" in version else "pypi"
    result = {"schema_version": 1, "provenance": gate.PROVENANCE,
              "producer": {"name": "northpole", "source_sha": "5" * 40},
              "package": package, "package_name": gate.PACKAGES[package], "version": version,
              "tag": ("test-v" if index == "testpypi" else "v") + version, "package_index": index,
              "sources": sources, "private_source_sha": sources[package],
              "tree_fingerprint_v1": tree.tree_fingerprint_v1(repo, commit_sha),
              "candidate_trees": {name: tree.tree_fingerprint_v1(repo, commit_sha)
                                  if name == package else "b" * 64 for name in ("sdk", "mcp")},
              "release_scope": "both", "release_order": ["sdk", "mcp"],
              "bundle": None, "bundle_digest": None, "peer_verification": "bootstrap-rc",
              "fixture_sha256": "6" * 64, "created_at": STAMP,
              "perimeters": dict.fromkeys(gate.SOURCES, "PASS"), "rc": None,
              "rollback": {"version": "1.1.0", "source_sha": "7" * 40,
                           "procedure": "pin-certified-version"}, "evidence": {}}
    for kind in ("ci", "candidate_canary"):
        result["evidence"][kind] = gate.inline_report(gate.canonical(report(kind, result)))
    return result


def tag(repo: Path, payload: dict, *, bad_hash: bool = False) -> None:
    sealed = gate.seal(payload)
    if bad_hash:
        sealed["content_sha256"] = "0" * 64
    git(repo, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "tag",
        "-a", "--cleanup=verbatim", payload["tag"], "-m", gate.canonical(sealed).decode())


def final_negative(repo: Path, package: str, rc_sha: str, rc: dict, case: str) -> tuple[str, dict]:
    tag(repo, rc)
    sources = {**SOURCES, package: "9" * 40}
    if case == "diff-outside-policy":
        (repo / "code.py").write_text("VALUE = 2\n")
    final_sha = commit(repo, package, "1.2.3", sources[package])
    final = manifest(repo, package, "1.2.3", final_sha, sources)
    final["peer_verification"] = ("peer-stable-declared-only-until-second-gate" if package == "sdk" else
                                  "previous-source-validated-and-version-exists-artifact-identity-not-verified")
    oracle = report("rc_oracle", final, rc)
    if case == "oracle-fail":
        oracle["status"] = "FAIL"
    final["evidence"]["rc_oracle"] = gate.inline_report(gate.canonical(oracle))
    entries = {name: {"tag": "test-v1.2.3rc9" if case == "rc-missing" and name == package else rc["tag"],
                      "public_sha": rc_sha if name == package else "c" * 40,
                      "private_sha": SOURCES[name], "tree_fingerprint_v1": rc["candidate_trees"][name],
                      "oracle_sha256": final["evidence"]["rc_oracle"]["sha256"]} for name in ("sdk", "mcp")}
    final["rc"] = {key: entries[package][key] for key in ("tag", "public_sha", "oracle_sha256")}
    body = {"version": final["version"], "package_index": "pypi", "release_order": ["sdk", "mcp"],
            "policy": "version-fields-v1", "sources": sources, "candidate_trees": final["candidate_trees"],
            "rc": entries, "canary_sha256": final["evidence"]["candidate_canary"]["sha256"],
            "partial_release": "first may publish while second is blocked; rollback by certified pin",
            "rollback": {name: final["rollback"] for name in ("sdk", "mcp")}}
    final.update(bundle=body, bundle_digest=hashlib.sha256(gate.canonical(body)).hexdigest())
    return final_sha, final


def run_case(package: str, case: str) -> int:
    with tempfile.TemporaryDirectory(prefix="offline-readiness-fixture-") as directory:
        repo = Path(directory)
        git(repo, "init", "-q")
        (repo / "code.py").write_text("VALUE = 1\n")
        commit_sha = commit(repo, package, "1.2.3rc1", SOURCES[package])
        payload = manifest(repo, package, "1.2.3rc1", commit_sha, SOURCES)
        if case in {"rc-missing", "oracle-fail", "diff-outside-policy"}:
            commit_sha, payload = final_negative(repo, package, commit_sha, payload, case)
        elif case == "missing-canary":
            del payload["evidence"]["candidate_canary"]
        elif case == "canary-fail":
            failed = report("candidate_canary", payload)
            failed["status"] = "FAIL"
            payload["evidence"]["candidate_canary"] = gate.inline_report(gate.canonical(failed))
        elif case == "sha-mismatch":
            payload["private_source_sha"] = "8" * 40
        elif case == "version-mismatch":
            payload["version"] = "9.9.9rc1"
        if case == "missing-manifest":
            git(repo, "tag", payload["tag"])  # Synthetic manual tag without any manifest.
        else:
            tag(repo, payload, bad_hash=case == "hash-mismatch")
        args = ["--repo", str(repo), "--package", package, "--tag", payload["tag"],
                "--index", payload["package_index"], "--commit", commit_sha]
        if payload["package_index"] == "pypi":
            # Self-negative fixtures must never contact a real peer on regression.
            with patch.object(gate, "verify_peer", lambda manifest, **kw: gate.validate_bundle(manifest)):
                return gate.main(args)
        return gate.main(args)


def expect_block(package: str, case: str) -> int:
    if case not in EXPECTED_BLOCKS:
        print("FAIL: a declared negative fixture is required", file=sys.stderr)
        return 1
    stdout, stderr = io.StringIO(), io.StringIO()
    with redirect_stdout(stdout), redirect_stderr(stderr):
        result = run_case(package, case)
    expected = "BLOCK: " + EXPECTED_BLOCKS[case]
    if result != 1 or stderr.getvalue().splitlines() != [expected] or "PASS:" in stdout.getvalue():
        print(f"FAIL: {case}; gate_exit={result}; expected reason not established", file=sys.stderr)
        return 1
    print(f"PASS: expected block {case}; gate_exit=1; reason={EXPECTED_BLOCKS[case]}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", choices=["sdk", "mcp"], required=True)
    parser.add_argument("--case", choices=["positive", "missing-manifest", "missing-canary",
                                          "canary-fail", "sha-mismatch", "version-mismatch",
                                          "hash-mismatch", "rc-missing", "oracle-fail",
                                          "diff-outside-policy"], required=True)
    parser.add_argument("--expect-block", action="store_true",
                        help="Pass the test only on gate exit 1 AND the exact expected reason")
    args = parser.parse_args(argv)
    if args.expect_block:
        return expect_block(args.package, args.case)
    return run_case(args.package, args.case)


if __name__ == "__main__":
    raise SystemExit(main())
