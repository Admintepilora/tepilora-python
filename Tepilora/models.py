from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class V3Request:
    action: str
    params: Dict[str, Any] = field(default_factory=dict)
    options: Optional[Dict[str, Any]] = None
    context: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"action": self.action, "params": self.params}
        if self.options is not None:
            payload["options"] = self.options
        if self.context is not None:
            payload["context"] = self.context
        return payload


@dataclass(frozen=True)
class V3Meta:
    request_id: Optional[str] = None
    execution_time_ms: Optional[int] = None
    timestamp: Optional[str] = None
    cache_hit: Optional[bool] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "V3Meta":
        known_keys = {"request_id", "execution_time_ms", "timestamp", "cache_hit"}
        extra = {k: v for k, v in data.items() if k not in known_keys}
        return V3Meta(
            request_id=(str(data["request_id"]) if "request_id" in data and data["request_id"] is not None else None),
            execution_time_ms=(
                int(data["execution_time_ms"]) if "execution_time_ms" in data and data["execution_time_ms"] is not None else None
            ),
            timestamp=(str(data["timestamp"]) if "timestamp" in data and data["timestamp"] is not None else None),
            cache_hit=(bool(data["cache_hit"]) if "cache_hit" in data and data["cache_hit"] is not None else None),
            extra=extra,
        )


@dataclass(frozen=True)
class V3Response:
    success: bool
    action: str
    data: Any
    meta: V3Meta

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "V3Response":
        meta_raw = data.get("meta")
        meta_dict = meta_raw if isinstance(meta_raw, dict) else {}
        return V3Response(
            success=bool(data.get("success", True)),
            action=str(data.get("action", "")),
            data=data.get("data"),
            meta=V3Meta.from_dict(meta_dict),
        )


@dataclass(frozen=True)
class V3BinaryMeta:
    request_id: Optional[str] = None
    execution_time_ms: Optional[int] = None
    total_count: Optional[int] = None
    row_count: Optional[int] = None


@dataclass(frozen=True)
class V3BinaryResponse:
    action: str
    format: str
    content_type: str
    content: bytes
    meta: V3BinaryMeta
    headers: Dict[str, str]
