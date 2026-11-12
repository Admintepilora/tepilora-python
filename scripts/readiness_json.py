"""Strict JSON and byte-level evidence hashing shared by readiness consumers."""

from __future__ import annotations

import base64
import hashlib
import json

try:
    from .readiness_tree import ReadinessError, require
except ImportError:
    from readiness_tree import ReadinessError, require


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True, allow_nan=False).encode("ascii")


def _pairs(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate JSON key")
        result[key] = value
    return result


def _nonfinite(value: str) -> None:
    raise ReadinessError("non-finite JSON number")


def load_json(raw: bytes) -> dict:
    require(0 < len(raw) <= 2_000_000, "missing or oversized JSON")
    result = json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs,
                        parse_constant=_nonfinite)
    require(isinstance(result, dict), "JSON object required")
    return result


def seal(payload: dict) -> dict:
    require("content_sha256" not in payload, "manifest is already sealed")
    return {**payload, "content_sha256": hashlib.sha256(canonical(payload)).hexdigest()}


def inline_report(raw: bytes) -> dict:
    load_json(raw)
    return {"report_base64": base64.b64encode(raw).decode("ascii"),
            "sha256": hashlib.sha256(raw).hexdigest()}
