"""Ordered SDK -> MCP validation; explicitly not an atomic pair publication."""

from __future__ import annotations

import hashlib
import os
import re
import subprocess
import tempfile
from contextlib import contextmanager
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import HTTPRedirectHandler, Request, build_opener

try:
    from .readiness_json import canonical, load_json
    from .readiness_tree import (
        annotation, digest, equivalent_versions, git, private_source, require, sha,
        tag_commit, tree_fingerprint_v1, rollback_pin,
    )
except ImportError:
    from readiness_json import canonical, load_json
    from readiness_tree import (
        annotation, digest, equivalent_versions, git, private_source, require, sha,
        tag_commit, tree_fingerprint_v1, rollback_pin,
    )


ORDER = ["sdk", "mcp"]
MIRRORS = {"sdk": "tepilora-python", "mcp": "tepilora-mcp-python"}
PACKAGES = {"sdk": "Tepilora", "mcp": "tepilora-mcp"}
RC_KEYS = {"tag", "public_sha", "private_sha", "tree_fingerprint_v1", "oracle_sha256"}


def candidate_trees(value: object) -> dict:
    require(isinstance(value, dict) and set(value) == set(ORDER), "two candidate tree fingerprints required")
    for item in value.values():
        digest(item)
    return value


def validate_bundle(manifest: dict) -> None:
    require(manifest["release_order"] == ORDER, "unsupported release order")
    require(manifest["release_scope"] in {"sdk", "mcp", "both"}, "release scope missing")
    require(manifest["release_scope"] in {manifest["package"], "both"}, "release scope/package mismatch")
    require(candidate_trees(manifest["candidate_trees"])[manifest["package"]]
            == manifest["tree_fingerprint_v1"], "own candidate fingerprint mismatch")
    if manifest["package_index"] == "testpypi":
        require(manifest["bundle"] is None and manifest["bundle_digest"] is None
                and manifest["peer_verification"] == "bootstrap-rc", "invalid RC bootstrap policy")
        return
    require(manifest["release_scope"] == "both", "single-target final publication is not supported")
    body = manifest["bundle"]
    require(isinstance(body, dict) and set(body) == {
        "version", "package_index", "release_order", "policy", "sources", "candidate_trees",
        "rc", "canary_sha256", "partial_release", "rollback"}, "complete common bundle required")
    require(hashlib.sha256(canonical(body)).hexdigest() == digest(manifest["bundle_digest"]),
            "common bundle hash mismatch")
    require(body["version"] == manifest["version"] and body["package_index"] == manifest["package_index"]
            and body["release_order"] == ORDER and body["policy"] == "version-fields-v1"
            and body["sources"] == manifest["sources"]
            and body["candidate_trees"] == manifest["candidate_trees"]
            and body["canary_sha256"] == manifest["evidence"]["candidate_canary"]["sha256"],
            "common bundle context mismatch")
    require(body["partial_release"] == "first may publish while second is blocked; rollback by certified pin",
            "partial release limitation must be explicit")
    require(isinstance(body["rollback"], dict) and set(body["rollback"]) == set(ORDER)
            and body["rollback"][manifest["package"]] == manifest["rollback"], "common rollback pins missing")
    for name in ORDER:
        rollback_pin(body["rollback"][name], manifest["version"], manifest["sources"][name])
    require(isinstance(body["rc"], dict) and set(body["rc"]) == set(ORDER), "two RC references required")
    for entry in body["rc"].values():
        require(isinstance(entry, dict) and set(entry) == RC_KEYS, "invalid bundle RC entry")
        sha(entry["public_sha"])
        sha(entry["private_sha"])
        digest(entry["tree_fingerprint_v1"])
        digest(entry["oracle_sha256"])
    own = body["rc"][manifest["package"]]
    require(manifest["rc"] == {key: own[key] for key in ("tag", "public_sha", "oracle_sha256")},
            "own RC reference differs from bundle")
    expected = ("peer-stable-declared-only-until-second-gate" if manifest["package"] == ORDER[0]
                else "previous-source-validated-and-version-exists-artifact-identity-not-verified")
    require(manifest["peer_verification"] == expected, "peer verification role mismatch")


def fetch_public_refs(repo: Path, package: str, tags: list[str]) -> None:
    require(package in MIRRORS, "unknown public mirror")
    refspecs = []
    for tag in tags:
        require(isinstance(tag, str) and re.fullmatch(r"(?:test-v|v)[0-9][0-9A-Za-z.]*", tag),
                "invalid public tag")
        refspecs.append(f"refs/tags/{tag}:refs/tags/{tag}")
    env = {key: value for key, value in os.environ.items()
           if key not in {"GH_TOKEN", "GITHUB_TOKEN", "RELEASE_BOT_TOKEN", "GIT_ASKPASS", "SSH_ASKPASS"}
           and not key.startswith("GIT_CONFIG_")}
    env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull, GIT_TERMINAL_PROMPT="0")
    result = subprocess.run(
        ["git", "-C", str(repo), "fetch", "--no-tags", "https://github.com/Admintepilora/"
         + MIRRORS[package] + ".git", *refspecs],
        env=env, capture_output=True, check=False, timeout=60,
    )
    require(result.returncode == 0, "anonymous public mirror unavailable")


@contextmanager
def public_mirror(package: str, tags: list[str]):
    with tempfile.TemporaryDirectory(prefix="readiness-public-") as directory:
        repo = Path(directory)
        git(repo, "init", "-q")
        fetch_public_refs(repo, package, tags)
        yield repo


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def index_version_exists(package_name: str, version: str, index: str) -> bool:
    require(index in {"pypi", "testpypi"}, "unknown index")
    host = "https://pypi.org" if index == "pypi" else "https://test.pypi.org"
    normalized_name = package_name.lower().replace("_", "-")
    url = f"{host}/pypi/{quote(normalized_name, safe='')}/{quote(version, safe='')}/json"
    try:
        with build_opener(_NoRedirect).open(Request(url, headers={"Accept": "application/json"}), timeout=15) as response:
            require(response.status == 200 and response.geturl() == url, "index response not definitive")
            payload = load_json(response.read(2_000_001))
    except (HTTPError, URLError, TimeoutError, OSError):
        return False
    name = str(payload.get("info", {}).get("name", "")).lower().replace("_", "-")
    return (name == package_name.lower().replace("_", "-")
            and payload.get("info", {}).get("version") == version and bool(payload.get("urls")))


def verify_peer(manifest: dict, *, inspect_rc, inspect_final, mirror_reader=None, index_reader=None) -> None:
    validate_bundle(manifest)
    body = manifest["bundle"]
    other = "mcp" if manifest["package"] == "sdk" else "sdk"
    entry = body["rc"][other]
    stable_tag = "v" + manifest["version"]
    tags = [entry["tag"]] + ([stable_tag] if manifest["package"] == "mcp" else [])
    with (mirror_reader or public_mirror)(other, tags) as repo:
        rc = inspect_rc(repo, other, entry, manifest)
        require(all(rc["sources"][name] == body["rc"][name]["private_sha"] for name in ORDER),
                "peer RC bundle source mismatch")
        require(all(rc["candidate_trees"][name] == body["rc"][name]["tree_fingerprint_v1"] for name in ORDER),
                "peer RC bundle tree mismatch")
        if manifest["package"] == "sdk":
            return  # Explicit first-gate limit: the peer's final export does not exist yet.
        public_sha = tag_commit(repo, stable_tag)
        first = inspect_final(repo, other, stable_tag, manifest["package_index"], public_sha)
        require(first["bundle_digest"] == manifest["bundle_digest"] and first["bundle"] == body,
                "previous stable tag belongs to another bundle")
        require((index_reader or index_version_exists)(PACKAGES[other], manifest["version"],
                                                       manifest["package_index"]),
                "previous package version absent or index unavailable")
