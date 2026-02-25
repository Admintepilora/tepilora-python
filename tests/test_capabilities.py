"""Tests for capabilities discovery."""

import json
from pathlib import Path

import pytest
from Tepilora import (
    TepiloraClient,
    capabilities,
    list_namespaces,
    list_operations,
    get_operation_info,
)
from conftest import _load_schema


SCHEMA = _load_schema()
HIDDEN_CATEGORIES = {"audit"}


def _non_internal_ops():
    return {
        action: op
        for action, op in SCHEMA["operations"].items()
        if not op.get("internal") and op["category"] not in HIDDEN_CATEGORIES
    }


def _expected_total_ops():
    return len(_non_internal_ops())


def _expected_namespaces():
    return sorted({op["category"] for op in _non_internal_ops().values()})


def _expected_ops_for_namespace(namespace: str):
    return [
        action
        for action, op in _non_internal_ops().items()
        if op["category"] == namespace
    ]


def test_schema_json_matches_embedded():
    """Ensure _schema.py is in sync with schema/registry.json."""
    from Tepilora._schema import SCHEMA

    schema_path = Path(__file__).parent.parent / "schema" / "registry.json"
    if not schema_path.exists():
        pytest.skip("schema/registry.json not available (installed from wheel)")
    with open(schema_path, "r", encoding="utf-8") as f:
        json_schema = json.load(f)
    assert SCHEMA["version"] == json_schema["version"]
    assert set(SCHEMA["operations"].keys()) == set(json_schema["operations"].keys())


class TestCapabilities:
    """Test capabilities functions."""

    def test_capabilities_summary(self):
        """Test full summary output."""
        result = capabilities()
        total_ops = _expected_total_ops()
        total_ns = len(_expected_namespaces())
        assert "TepiloraSDK" in result
        assert f"{total_ops} operations" in result
        assert f"{total_ns} namespaces" in result
        assert "analytics" in result

    def test_capabilities_namespace(self):
        """Test namespace detail output."""
        result = capabilities("portfolio")
        portfolio_ops = len(_expected_ops_for_namespace("portfolio"))
        assert f"portfolio - {portfolio_ops} operations" in result
        assert "create" in result
        assert "delete" in result

    def test_capabilities_search(self):
        """Test search functionality."""
        result = capabilities(search="volatility")
        assert "rolling_volatility" in result
        assert "analytics" in result

    def test_capabilities_operation_detail(self):
        """Test specific operation details."""
        result = capabilities("analytics.rolling_volatility")
        assert "rolling_volatility" in result
        assert "Category: analytics" in result
        assert "Parameters:" in result

    def test_capabilities_dict_format(self):
        """Test dict output format."""
        result = capabilities(format="dict")
        assert isinstance(result, dict)
        assert "operations" in result
        assert "categories" in result

    def test_capabilities_unknown_namespace(self):
        """Test error message for unknown namespace."""
        result = capabilities("nonexistent")
        assert "not found" in result
        assert "Available:" in result

    def test_list_namespaces(self):
        """Test list_namespaces helper."""
        ns = list_namespaces()
        expected = _expected_namespaces()
        assert isinstance(ns, list)
        assert "analytics" in ns
        assert "portfolio" in ns
        assert len(ns) == len(expected)

    def test_list_operations(self):
        """Test list_operations helper."""
        ops = list_operations("esg")
        expected = _expected_ops_for_namespace("esg")
        assert isinstance(ops, list)
        assert "esg.compare" in ops
        assert len(ops) == len(expected)

    def test_list_operations_all(self):
        """Test list_operations without filter."""
        ops = list_operations()
        assert len(ops) == _expected_total_ops()

    def test_get_operation_info(self):
        """Test get_operation_info helper."""
        info = get_operation_info("portfolio.create")
        assert info is not None
        assert info["category"] == "portfolio"
        assert info["operation"] == "create"
        assert "params" in info

    def test_get_operation_info_unknown(self):
        """Test get_operation_info for unknown operation."""
        info = get_operation_info("nonexistent.op")
        assert info is None


class TestClientCapabilities:
    """Test capabilities method on client."""

    def test_client_capabilities_method_exists(self):
        """Test that client has capabilities method."""
        client = TepiloraClient(api_key="test")
        assert hasattr(client, "capabilities")
        assert callable(client.capabilities)

    def test_client_capabilities_returns_text(self):
        """Test client.capabilities with text format."""
        client = TepiloraClient(api_key="test")
        result = client.capabilities(format="text")
        assert isinstance(result, str)
        assert "TepiloraSDK" in result

    def test_client_capabilities_returns_dict(self):
        """Test client.capabilities with dict format."""
        client = TepiloraClient(api_key="test")
        result = client.capabilities(format="dict")
        assert isinstance(result, dict)
        assert "operations" in result
