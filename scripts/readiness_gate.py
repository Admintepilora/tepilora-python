"""Fail-closed, declarative release readiness. Python 3.11+; no network calls.

Copied unchanged to public package scripts together with readiness_tree.py.
The validator never executes commands supplied by a manifest.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    from .readiness_bundle import PACKAGES, candidate_trees, validate_bundle, verify_peer
    from .readiness_json import canonical, inline_report, load_json, seal
    from .readiness_tree import (
        ReadinessError, VERSION_RE, annotation, digest, equivalent_versions,
        git, private_source, require, sha, tag_commit, tree_fingerprint_v1,
        version_bytes, version_span, rollback_pin,
    )
except ImportError:
    from readiness_bundle import PACKAGES, candidate_trees, validate_bundle, verify_peer
    from readiness_json import canonical, inline_report, load_json, seal
    from readiness_tree import (
        ReadinessError, VERSION_RE, annotation, digest, equivalent_versions,
        git, private_source, require, sha, tag_commit, tree_fingerprint_v1,
        version_bytes, version_span, rollback_pin,
    )


PROVENANCE = "declarative, NOT authenticated"
SOURCES = {"libraries", "webserver", "sdk", "mcp"}
REPORT_KEYS = {
    "schema_version", "kind", "provenance", "status", "exit_code", "command",
    "run_id", "started_at", "finished_at", "sources", "candidate_trees",
    "fixture_sha256", "cases", "upstream_pins",
}


def utc(value: object) -> datetime:
    require(isinstance(value, str) and value.endswith("Z"), "UTC timestamp required")
    parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    require(parsed.tzinfo is not None, "UTC timestamp required")
    return parsed


def source_map(value: object) -> dict:
    require(isinstance(value, dict) and set(value) == SOURCES, "all four source SHAs required")
    for item in value.values():
        sha(item)
    return value


def _pass_scopes(value: object, scopes: set[str]) -> None:
    require(isinstance(value, dict) and set(value) == scopes, "required perimeters missing")
    require(all(status == "PASS" for status in value.values()), "non-PASS perimeter")


def verify_report(kind: str, wrapper: object, manifest: dict, *,
                  sources: dict | None = None, trees: dict | None = None) -> dict:
    require(isinstance(wrapper, dict) and set(wrapper) == {"report_base64", "sha256"},
            "complete inline report required")
    digest(wrapper["sha256"])
    require(isinstance(wrapper["report_base64"], str), "inline report must be base64")
    raw = base64.b64decode(wrapper["report_base64"], validate=True)
    require(hashlib.sha256(raw).hexdigest() == wrapper["sha256"], "report hash mismatch")
    report = load_json(raw)
    require(set(report) == REPORT_KEYS, "unsupported or incomplete report")
    require(type(report["schema_version"]) is int and report["schema_version"] == 1,
            "unsupported report schema")
    require(report["kind"] == kind and report["provenance"] == PROVENANCE,
            "report kind or provenance mismatch")
    require(report["upstream_pins"] == "declared release context, NOT proof of deploy",
            "upstream source assurance must be explicit")
    require(report["status"] == "PASS" and type(report["exit_code"]) is int
            and report["exit_code"] == 0, f"{kind}: report did not pass")
    require(source_map(report["sources"]) == (sources or manifest["sources"]),
            "report source mismatch")
    require(candidate_trees(report["candidate_trees"]) == (trees or manifest["candidate_trees"]),
            "report candidate tree mismatch")
    require(report["fixture_sha256"] == manifest["fixture_sha256"], "report fixture mismatch")
    start, finish = utc(report["started_at"]), utc(report["finished_at"])
    require(start <= finish <= utc(manifest["created_at"]), "report timestamp mismatch")
    require(isinstance(report["run_id"], str)
            and re.fullmatch(r"[A-Za-z0-9_.:-]{1,120}", report["run_id"]), "run identifier missing")
    command = report["command"]
    require(isinstance(command, list) and 0 < len(command) <= 32, "declared command missing")
    for arg in command:
        require(isinstance(arg, str) and re.fullmatch(r"[A-Za-z0-9_.=:+/-]{1,160}", arg)
                and not arg.startswith("/")
                and not re.search(r"(?i)token|secret|password|api.?key|://|10\.0\.", arg),
                "command must be sanitized metadata")
    scopes = {manifest["package"]} if kind == "ci" else SOURCES
    cases = report["cases"]
    require(isinstance(cases, list) and cases, "empty report cases")
    seen = set()
    for case in cases:
        require(isinstance(case, dict) and set(case) == {"id", "status", "perimeters", "hashes"},
                "invalid report case")
        require(isinstance(case["id"], str) and re.fullmatch(r"[A-Za-z0-9_.:-]{1,120}", case["id"])
                and case["id"] not in seen,
                "missing or duplicate case identifier")
        seen.add(case["id"])
        require(case["status"] == "PASS", "non-PASS case")
        _pass_scopes(case["perimeters"], scopes)
        require(isinstance(case["hashes"], dict) and set(case["hashes"]) == scopes,
                "case comparison hashes missing")
        for value in case["hashes"].values():
            digest(value)
        if kind != "ci":
            require(len(set(case["hashes"].values())) == 1, "cross-stack comparison differs")
    return report


def validate_manifest(manifest: dict, package: str, tag: str, index: str) -> dict:
    require(package in PACKAGES and index in {"pypi", "testpypi"}, "unknown publication target")
    keys = {"schema_version", "provenance", "producer", "package", "package_name",
            "version", "tag", "package_index", "private_source_sha", "sources",
            "tree_fingerprint_v1", "fixture_sha256", "created_at", "perimeters",
            "evidence", "rollback", "rc", "content_sha256", "candidate_trees", "release_order",
            "release_scope", "bundle", "bundle_digest", "peer_verification"}
    require(set(manifest) == keys, "unsupported or incomplete readiness manifest")
    require(type(manifest["schema_version"]) is int and manifest["schema_version"] == 1,
            "unsupported manifest schema")
    require(manifest["provenance"] == PROVENANCE, "declarative provenance label required")
    expected_hash = digest(manifest["content_sha256"])
    require(hashlib.sha256(canonical({k: v for k, v in manifest.items()
                                     if k != "content_sha256"})).hexdigest() == expected_hash,
            "manifest hash mismatch")
    producer = manifest["producer"]
    require(isinstance(producer, dict) and set(producer) == {"name", "source_sha"}
            and producer["name"] == "northpole", "producer metadata missing")
    sha(producer["source_sha"])
    require(manifest["package"] == package and manifest["package_name"] == PACKAGES[package]
            and manifest["tag"] == tag and manifest["package_index"] == index,
            "package, tag or index mismatch")
    version = manifest["version"]
    require(isinstance(version, str) and VERSION_RE.fullmatch(version), "invalid package version")
    prefix = "test-v" if index == "testpypi" else "v"
    require(tag == prefix + version, "version does not match tag")
    final = re.fullmatch(r"(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)", version)
    require(bool(final) == (index == "pypi"), "version does not belong to publication lane")
    sources = source_map(manifest["sources"])
    require(sha(manifest["private_source_sha"]) == sources[package], "private source mismatch")
    digest(manifest["tree_fingerprint_v1"])
    digest(manifest["fixture_sha256"])
    require(utc(manifest["created_at"]) <= datetime.now(timezone.utc) + timedelta(minutes=5),
            "manifest timestamp is in the future")
    _pass_scopes(manifest["perimeters"], SOURCES)
    rollback = manifest["rollback"]
    rollback_pin(rollback, version, sources[package])
    stages = {"ci", "candidate_canary"} | ({"rc_oracle"} if index == "pypi" else set())
    require(isinstance(manifest["evidence"], dict) and set(manifest["evidence"]) == stages,
            "required prepublication evidence missing")
    for stage in ("ci", "candidate_canary"):
        verify_report(stage, manifest["evidence"][stage], manifest)
    if index == "testpypi":
        require(manifest["rc"] is None, "RC upload must not depend on its own future oracle")
    validate_bundle(manifest)
    return manifest


def verify_public_tree(manifest: dict, repo: str | Path, commit: str) -> None:
    sha(commit)
    require(private_source(repo, commit) == manifest["private_source_sha"], "source trailer mismatch")
    require(tree_fingerprint_v1(repo, commit) == manifest["tree_fingerprint_v1"],
            "exported tree fingerprint mismatch")
    version_span(manifest["package"], version_bytes(repo, commit, manifest["package"]),
                 manifest["version"])


def verify_release_rc(manifest: dict, repo: str | Path, commit: str) -> None:
    ref = manifest["rc"]
    require(isinstance(ref, dict) and set(ref) == {"tag", "public_sha", "oracle_sha256"},
            "validated RC reference missing")
    rc_sha = sha(ref["public_sha"])
    try:
        actual_rc_sha = tag_commit(repo, ref["tag"])
    except ReadinessError:
        raise ReadinessError("referenced RC tag missing or invalid") from None
    require(actual_rc_sha == rc_sha, "RC tag SHA mismatch")
    rc = validate_manifest(load_json(annotation(repo, ref["tag"])), manifest["package"],
                           ref["tag"], "testpypi")
    verify_public_tree(rc, repo, rc_sha)
    require(rc["fixture_sha256"] == manifest["fixture_sha256"], "RC fixture changed")
    for source in {"libraries", "webserver"}:
        require(rc["sources"][source] == manifest["sources"][source], "RC cross-stack source changed")
    require(digest(ref["oracle_sha256"]) == manifest["evidence"]["rc_oracle"]["sha256"],
            "RC oracle reference hash mismatch")
    verify_report("rc_oracle", manifest["evidence"]["rc_oracle"], manifest,
                  sources=rc["sources"], trees=rc["candidate_trees"])
    own = manifest["bundle"]["rc"][manifest["package"]]
    require(own["private_sha"] == rc["private_source_sha"]
            and own["tree_fingerprint_v1"] == rc["tree_fingerprint_v1"], "own RC bundle mismatch")
    require(all(rc["sources"][name] == manifest["bundle"]["rc"][name]["private_sha"]
                for name in ("sdk", "mcp")), "own RC cross-stack source mismatch")
    require(all(rc["candidate_trees"][name] == manifest["bundle"]["rc"][name]["tree_fingerprint_v1"]
                for name in ("sdk", "mcp")), "own RC cross-stack tree mismatch")
    equivalent_versions(repo, rc_sha, commit, manifest["package"], rc["version"], manifest["version"])


def verify_tag(repo: str | Path, package: str, tag: str, index: str, commit: str) -> dict:
    require(tag_commit(repo, tag) == sha(commit), "tag commit does not match validated checkout")
    require(git(repo, "rev-parse", "HEAD").decode().strip() == commit, "checkout is not the tag commit")
    manifest = validate_manifest(load_json(annotation(repo, tag)), package, tag, index)
    verify_public_tree(manifest, repo, commit)
    if index == "pypi":
        verify_release_rc(manifest, repo, commit)
        verify_peer(manifest, inspect_rc=inspect_rc, inspect_final=inspect_final)
    return manifest


def inspect_rc(repo: Path, package: str, entry: dict, manifest: dict) -> dict:
    require(tag_commit(repo, entry["tag"]) == entry["public_sha"], "public RC SHA mismatch")
    rc = validate_manifest(load_json(annotation(repo, entry["tag"])), package, entry["tag"], "testpypi")
    verify_public_tree(rc, repo, entry["public_sha"])
    require(rc["private_source_sha"] == entry["private_sha"]
            and rc["tree_fingerprint_v1"] == entry["tree_fingerprint_v1"]
            and rc["fixture_sha256"] == manifest["fixture_sha256"], "bundle RC source mismatch")
    for name in ("libraries", "webserver"):
        require(rc["sources"][name] == manifest["sources"][name], "RC upstream source changed")
    return rc


def inspect_final(repo: Path, package: str, tag: str, index: str, commit: str) -> dict:
    require(tag_commit(repo, tag) == commit, "previous stable commit changed")
    manifest = validate_manifest(load_json(annotation(repo, tag)), package, tag, index)
    verify_public_tree(manifest, repo, commit)
    verify_release_rc(manifest, repo, commit)
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=".")
    parser.add_argument("--package", choices=sorted(PACKAGES), required=True)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--index", choices=["pypi", "testpypi"], required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--repository", help="Actual repository identity supplied by the publish workflow")
    args = parser.parse_args(argv)
    try:
        if args.repository is not None:
            from_repo = {"sdk": "Admintepilora/tepilora-python", "mcp": "Admintepilora/tepilora-mcp-python"}
            require(args.repository == from_repo[args.package], "workflow repository/package mismatch")
        verify_tag(args.repo, args.package, args.tag, args.index, args.commit)
    except ReadinessError as error:
        print(f"BLOCK: {error}", file=sys.stderr)
        return 1
    except (ValueError, TypeError, KeyError, UnicodeError, SyntaxError,
            OSError, subprocess.SubprocessError) as error:
        print(f"BLOCK: readiness validation failed ({type(error).__name__})", file=sys.stderr)
        return 1
    print("PASS: release readiness (declarative, NOT authenticated)")
    if args.index == "pypi":
        print("LIMIT: no atomic pair or artifact identity proof; first gate does not verify peer stable export")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
