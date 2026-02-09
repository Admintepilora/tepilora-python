"""Tests for capabilities discovery."""

import pytest
from Tepilora import (
    TepiloraClient,
    capabilities,
    list_namespaces,
    list_operations,
    get_operation_info,
)


class TestCapabilities:
    """Test capabilities functions."""

    def test_capabilities_summary(self):
        """Test full summary output."""
        result = capabilities()
        assert "TepiloraSDK" in result
        assert "218 operations" in result
        assert "23 namespaces" in result
        assert "analytics" in result

    def test_capabilities_namespace(self):
        """Test namespace detail output."""
        result = capabilities("portfolio")
        assert "portfolio - 19 operations" in result
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
        assert isinstance(ns, list)
        assert "analytics" in ns
        assert "portfolio" in ns
        assert len(ns) == 23

    def test_list_operations(self):
        """Test list_operations helper."""
        ops = list_operations("esg")
        assert isinstance(ops, list)
        assert "esg.compare" in ops
        assert len(ops) == 5

    def test_list_operations_all(self):
        """Test list_operations without filter."""
        ops = list_operations()
        assert len(ops) == 218

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
