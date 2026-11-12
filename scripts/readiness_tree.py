"""Read-only git inspection and the frozen version-only equivalence policy."""

from __future__ import annotations

import ast
import hashlib
import re
import subprocess
import tomllib
from pathlib import Path


class ReadinessError(ValueError):
    """Publication cannot establish a required readiness condition."""


VERSION_POLICY_V1 = {
    "sdk": "Tepilora/version.py",
    "mcp": "pyproject.toml",
}
SHA_RE = re.compile(r"[0-9a-f]{40}")
DIGEST_RE = re.compile(r"[0-9a-f]{64}")
VERSION_RE = re.compile(r"(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(rc\d+|\.dev\d+)?")


def require(condition: object, message: str) -> None:
    if not condition:
        raise ReadinessError(message)


def git(repo: str | Path, *args: str) -> bytes:
    result = subprocess.run(
        ["git", "--no-replace-objects", "-C", str(repo), *args],
        capture_output=True, check=False, timeout=30,
    )
    require(result.returncode == 0, "required git inspection failed")
    return result.stdout


def sha(value: object) -> str:
    require(isinstance(value, str) and SHA_RE.fullmatch(value), "invalid full source SHA")
    return value


def digest(value: object) -> str:
    require(isinstance(value, str) and DIGEST_RE.fullmatch(value), "invalid SHA256")
    return value


def rollback_pin(value: object, current_version: str, current_source: str) -> None:
    require(isinstance(value, dict) and set(value) == {"version", "source_sha", "procedure"},
            "rollback pin missing")
    require(isinstance(value["version"], str) and VERSION_RE.fullmatch(value["version"])
            and value["procedure"] == "pin-certified-version", "certified rollback pin required")
    sha(value["source_sha"])
    require(value["version"] != current_version and value["source_sha"] != current_source,
            "rollback must reference a different certified version and source")


def tree_fingerprint_v1(repo: str | Path, treeish: str) -> str:
    sha(treeish)
    raw = git(repo, "ls-tree", "-r", "-z", "--full-tree", treeish)
    require(raw, "empty exported tree")
    require(all(b" blob " in record.split(b"\t", 1)[0]
                for record in raw.split(b"\0") if record), "non-blob exported entry")
    return hashlib.sha256(raw).hexdigest()


def tag_commit(repo: str | Path, tag: str) -> str:
    require(re.fullmatch(r"(?:test-v|v)[0-9][0-9A-Za-z.]*", tag), "invalid tag name")
    return sha(git(repo, "rev-parse", "--verify", f"refs/tags/{tag}^{{commit}}")
               .decode("ascii").strip())


def annotation(repo: str | Path, tag: str) -> bytes:
    tag_commit(repo, tag)
    ref = f"refs/tags/{tag}"
    require(git(repo, "cat-file", "-t", ref) == b"tag\n", "annotated tag required")
    header, separator, payload = git(repo, "cat-file", "tag", ref).partition(b"\n\n")
    require(separator and payload, "missing tag annotation")
    require(b"type commit" in header.splitlines(), "tag must point directly to a commit")
    require(b"tag " + tag.encode("ascii") in header.splitlines(), "tag name mismatch")
    return payload


def private_source(repo: str | Path, commit: str) -> str:
    sha(commit)
    message = git(repo, "show", "-s", "--format=%B", commit)
    # Only the final trailer block counts; a body line must not impersonate it.
    parsed = subprocess.run(
        ["git", "interpret-trailers", "--parse"], input=message,
        capture_output=True, check=False, timeout=30,
    )
    require(parsed.returncode == 0, "cannot inspect source trailer")
    values = [line.partition(b":")[2].strip() for line in parsed.stdout.splitlines()
              if line.partition(b":")[0].lower() == b"tepilora-source-sha"]
    require(len(values) == 1, "exactly one source trailer required")
    return sha(values[0].decode("ascii"))


def _tree(repo: str | Path, commit: str) -> dict[bytes, tuple[bytes, bytes, bytes]]:
    sha(commit)
    entries = {}
    for record in git(repo, "ls-tree", "-r", "-z", "--full-tree", commit).split(b"\0"):
        if record:
            metadata, path = record.split(b"\t", 1)
            mode, kind, oid = metadata.split(b" ")
            require(kind == b"blob", "non-blob exported entry")
            entries[path] = (mode, kind, oid)
    require(entries, "empty exported tree")
    return entries


def version_span(package: str, raw: bytes, expected: str) -> tuple[int, int]:
    """Locate only a simple, unescaped version value after structured parsing."""
    require(package in VERSION_POLICY_V1 and VERSION_RE.fullmatch(expected), "invalid version policy")
    text = raw.decode("utf-8")
    if package == "sdk":
        tree = ast.parse(text)
        assignments = [node for node in ast.walk(tree) if isinstance(node, ast.Assign)
                       and any(isinstance(t, ast.Name) and t.id == "__version__"
                               for t in node.targets)]
        require(len(assignments) == 1, "unique version assignment required")
        node = assignments[0]
        require(len(node.targets) == 1 and node in tree.body, "top-level version required")
        value = node.value
        require(isinstance(value, ast.Constant) and value.value == expected,
                "candidate version mismatch")
        lines = raw.splitlines(keepends=True)
        start = sum(map(len, lines[:value.lineno - 1])) + value.col_offset
        end = sum(map(len, lines[:value.end_lineno - 1])) + value.end_col_offset
        literal = raw[start:end]
        require(literal in (b'"' + expected.encode() + b'"',
                            b"'" + expected.encode() + b"'"), "simple version literal required")
        return start + 1, end - 1
    require(tomllib.loads(text).get("project", {}).get("version") == expected,
            "project version mismatch")
    sections = list(re.finditer(rb"(?m)^\[project\][ \t]*(?:#[^\r\n]*)?\r?$", raw))
    require(len(sections) == 1, "unique project section required")
    start = sections[0].end()
    following = re.search(rb"(?m)^\s*\[", raw[start:])
    end = start + following.start() if following else len(raw)
    pattern = rb"(?m)^[ \t]*version[ \t]*=[ \t]*([\"'])(" + re.escape(expected.encode())
    pattern += rb")(\1)[ \t]*(?:#[^\r\n]*)?\r?$"
    matches = list(re.finditer(pattern, raw[start:end]))
    require(len(matches) == 1, "simple project version field required")
    return start + matches[0].start(2), start + matches[0].end(2)


def version_bytes(repo: str | Path, commit: str, package: str) -> bytes:
    sha(commit)
    require(package in VERSION_POLICY_V1, "unknown package")
    return git(repo, "show", f"{commit}:{VERSION_POLICY_V1[package]}")


def equivalent_versions(repo: str | Path, rc_sha: str, release_sha: str,
                        package: str, rc_version: str, release_version: str) -> None:
    require(re.fullmatch(re.escape(release_version) + r"rc\d+", rc_version),
            "RC must belong to the final version")
    old, new = _tree(repo, rc_sha), _tree(repo, release_sha)
    require(old.keys() == new.keys(), "exported file set changed since RC")
    version_path = VERSION_POLICY_V1[package].encode()
    require(version_path in old, "version file missing")
    for path in old:
        require(old[path][:2] == new[path][:2], "exported file mode changed since RC")
        if path != version_path:
            require(old[path] == new[path], "non-version source changed since RC")
    before = version_bytes(repo, rc_sha, package)
    after = version_bytes(repo, release_sha, package)
    a, b = version_span(package, before, rc_version), version_span(package, after, release_version)
    require(before[:a[0]] + b"<version>" + before[a[1]:]
            == after[:b[0]] + b"<version>" + after[b[1]:],
            "version file changed outside the allowed value")
