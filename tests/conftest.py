"""
Shared pytest fixtures for TepiloraSDK tests.
"""

import json
import re
from pathlib import Path
from typing import Any, Dict, List

import httpx
import pytest

from Tepilora import TepiloraClient, AsyncTepiloraClient


SCHEMA_PATH = Path(__file__).parent.parent / "schema" / "registry.json"
V3_PREFIX = "/T-Api/v3"


def _load_schema() -> Dict[str, Any]:
    """Load schema from JSON file if available, otherwise from embedded schema."""
    if SCHEMA_PATH.exists():
        with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    from Tepilora._schema import SCHEMA
    return SCHEMA


def _payload_from_request(request: httpx.Request) -> Dict[str, Any]:
    """Parse JSON calls and provide structural payloads for multipart calls."""
    content_type = request.headers.get("Content-Type", "")
    if "multipart/form-data" in content_type and request.url.path.startswith(f"{V3_PREFIX}/"):
        rest = request.url.path[len(V3_PREFIX) + 1:]
        category, _, operation = rest.partition("/")
        field_names = [
            match.decode("utf-8")
            for match in re.findall(rb'name="([^"]+)"', request.content)
        ]
        return {
            "action": f"{category}.{operation}",
            "params": {name: "multipart" for name in field_names},
            "raw": request.content,
        }
    try:
        payload = json.loads(request.content.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return {"raw": request.content}
    if request.url.path.startswith(f"{V3_PREFIX}/") and request.url.path != V3_PREFIX:
        rest = request.url.path[len(V3_PREFIX) + 1:]
        category, _, operation = rest.partition("/")
        if category and operation and isinstance(payload, dict) and "action" not in payload:
            return {
                "action": f"{category}.{operation}",
                "params": payload,
            }
    return payload


def _mock_response_for_request(request: httpx.Request, payload: Dict[str, Any]) -> httpx.Response:
    action = payload.get("action", "unknown")
    if request.headers.get("Accept") == "application/octet-stream":
        return httpx.Response(
            200,
            headers={
                "Content-Type": "application/octet-stream",
                "Content-Disposition": 'attachment; filename="mock.bin"',
                "X-Tepilora-Request-Id": "test-123",
            },
            content=b"mock binary",
        )
    return httpx.Response(
        200,
        json={
            "success": True,
            "action": action,
            "data": {"result": "mock"},
            "meta": {"request_id": "test-123"},
        },
    )


@pytest.fixture(scope="session")
def schema() -> Dict[str, Any]:
    """Load registry schema (session-scoped for performance)."""
    return _load_schema()


@pytest.fixture(scope="session")
def all_operations(schema) -> Dict[str, Dict[str, Any]]:
    """All non-internal operations from schema."""
    return {
        action: op
        for action, op in schema["operations"].items()
        if not op.get("internal")
    }


@pytest.fixture
def mock_transport():
    """Create a mock transport that records calls."""
    calls: List[Dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        payload = _payload_from_request(request)

        calls.append({
            "method": request.method,
            "url": str(request.url),
            "payload": payload,
            "headers": dict(request.headers),
        })

        return _mock_response_for_request(request, payload)

    transport = httpx.MockTransport(handler)
    transport.calls = calls  # type: ignore
    return transport


@pytest.fixture
def mock_client(mock_transport) -> TepiloraClient:
    """TepiloraClient with mock transport."""
    client = TepiloraClient(
        api_key="test-api-key",
        base_url="http://test.local",
        transport=mock_transport,
    )
    client._calls = mock_transport.calls  # type: ignore
    return client


@pytest.fixture
def async_mock_transport():
    """Create an async mock transport that records calls."""
    calls: List[Dict[str, Any]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        payload = _payload_from_request(request)

        calls.append({
            "method": request.method,
            "url": str(request.url),
            "payload": payload,
        })

        return _mock_response_for_request(request, payload)

    transport = httpx.MockTransport(handler)
    transport.calls = calls  # type: ignore
    return transport


@pytest.fixture
def async_mock_client(async_mock_transport) -> AsyncTepiloraClient:
    """AsyncTepiloraClient with mock transport."""
    client = AsyncTepiloraClient(
        api_key="test-api-key",
        base_url="http://test.local",
        transport=async_mock_transport,
    )
    client._calls = async_mock_transport.calls  # type: ignore
    return client


def generate_test_value(type_name: str) -> Any:
    """Generate a test value for a given type."""
    return {
        "string": "test_value",
        "int": 42,
        "float": 3.14,
        "bool": True,
        "list": ["item1", "item2"],
        "dict": {"key": "value"},
        "file": b"test file payload",
    }.get(type_name, "test")


import keyword as _keyword

_PYTHON_KEYWORDS = set(_keyword.kwlist) | {"match", "case", "type"}

def _sanitize_param_name(name: str) -> str:
    """Mirror generate_sdk.py's sanitize_param_name for Python keywords."""
    if name in _PYTHON_KEYWORDS:
        return name + '_'
    return name

def build_minimal_params(params: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Build minimal params dict with only required params."""
    result = {}
    for p in params:
        if p.get("required") and p["name"] != "format":
            result[_sanitize_param_name(p["name"])] = generate_test_value(p["type"])
    return result


def build_all_params(params: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Build full optional payload projection params for wrapper canaries.

    This is a structural SDK coverage canary: it verifies generated wrappers
    accept and serialize every declared payload parameter. It does not assert
    V3 semantic validity for the generated test values.
    """
    result = {}
    for p in params:
        if p["name"] != "format":
            result[_sanitize_param_name(p["name"])] = generate_test_value(p["type"])
    return result
