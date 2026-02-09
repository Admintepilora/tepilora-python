"""
Shared pytest fixtures for TepiloraSDK tests.
"""

import json
from pathlib import Path
from typing import Any, Dict, List

import httpx
import pytest

from Tepilora import TepiloraClient, AsyncTepiloraClient


SCHEMA_PATH = Path(__file__).parent.parent / "schema" / "registry.json"


@pytest.fixture(scope="session")
def schema() -> Dict[str, Any]:
    """Load registry schema (session-scoped for performance)."""
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


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
        # Parse request
        try:
            payload = json.loads(request.content.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            payload = {"raw": request.content}

        calls.append({
            "method": request.method,
            "url": str(request.url),
            "payload": payload,
            "headers": dict(request.headers),
        })

        # Return success response
        action = payload.get("action", "unknown")
        return httpx.Response(
            200,
            json={
                "success": True,
                "action": action,
                "data": {"result": "mock"},
                "meta": {"request_id": "test-123"},
            },
        )

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
        try:
            payload = json.loads(request.content.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            payload = {"raw": request.content}

        calls.append({
            "method": request.method,
            "url": str(request.url),
            "payload": payload,
        })

        action = payload.get("action", "unknown")
        return httpx.Response(
            200,
            json={
                "success": True,
                "action": action,
                "data": {"result": "mock"},
                "meta": {"request_id": "test-123"},
            },
        )

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
    }.get(type_name, "test")


def build_minimal_params(params: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Build minimal params dict with only required params."""
    result = {}
    for p in params:
        if p.get("required") and p["name"] != "format":
            result[p["name"]] = generate_test_value(p["type"])
    return result
