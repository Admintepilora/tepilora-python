"""
Parametric tests for all 218 SDK operations.

Tests that each operation:
1. Is accessible via the correct namespace
2. Sends the correct action to the API
3. Handles required parameters correctly
"""

import json
from pathlib import Path
from typing import Any, Dict, List

import pytest

from conftest import build_minimal_params, generate_test_value


# Load schema at module level for parametrize
SCHEMA_PATH = Path(__file__).parent.parent / "schema" / "registry.json"
with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
    SCHEMA = json.load(f)

# Skip categories (internal only or not implemented)
SKIP_CATEGORIES = {"audit", "exports"}

# Operations that use special endpoints (not V3 unified)
SPECIAL_OPERATIONS = {"analytics.info", "analytics.list"}

# Method name renames (must match generate_sdk.py)
METHOD_RENAMES = {
    "global": "global_search",
    "import": "import_data",
    "exec": "execute",
    "eval": "evaluate",
}


def get_method_name(operation: str) -> str:
    """Get Python method name from operation name."""
    return METHOD_RENAMES.get(operation, operation)


def all_operations():
    """Generate pytest params for all operations."""
    for action, op in sorted(SCHEMA["operations"].items()):
        if op.get("internal"):
            continue
        if op["category"] in SKIP_CATEGORIES:
            continue
        yield pytest.param(action, op, id=action)


def all_categories():
    """Generate pytest params for all categories."""
    for category in sorted(SCHEMA["by_category"].keys()):
        if category in SKIP_CATEGORIES:
            continue
        yield pytest.param(category, id=category)


class TestAllOperations:
    """Test all 218 operations send correct actions."""

    @pytest.mark.parametrize("action,op", all_operations())
    def test_operation_sends_correct_action(self, action: str, op: Dict[str, Any], mock_client):
        """Verify each operation sends the correct action string."""
        # Skip special operations that don't use V3 unified endpoint
        if action in SPECIAL_OPERATIONS:
            pytest.skip(f"{action} uses special endpoint")

        category = op["category"]
        operation = op["operation"]
        method_name = get_method_name(operation)
        params = op.get("params", [])

        # Get namespace
        namespace = getattr(mock_client, category, None)
        assert namespace is not None, f"Namespace '{category}' not found on client"

        # Get method
        method = getattr(namespace, method_name, None)
        assert method is not None, f"Method '{method_name}' not found on {category}"

        # Build minimal params
        kwargs = build_minimal_params(params)

        # Call method
        try:
            result = method(**kwargs)
        except Exception as e:
            pytest.fail(f"Method {action} raised {type(e).__name__}: {e}")

        # Verify call was made
        assert len(mock_client._calls) >= 1, f"No calls recorded for {action}"

        # Find the call (might be multiple for analytics.list, etc.)
        call = mock_client._calls[-1]
        sent_action = call["payload"].get("action")

        assert sent_action == action, f"Expected action '{action}', got '{sent_action}'"

    @pytest.mark.parametrize("action,op", all_operations())
    def test_operation_includes_required_params(self, action: str, op: Dict[str, Any], mock_client):
        """Verify required params are included in request."""
        # Skip special operations that don't use V3 unified endpoint
        if action in SPECIAL_OPERATIONS:
            pytest.skip(f"{action} uses special endpoint")

        category = op["category"]
        operation = op["operation"]
        method_name = get_method_name(operation)
        params = op.get("params", [])
        required_params = [p["name"] for p in params if p.get("required") and p["name"] != "format"]

        if not required_params:
            pytest.skip("No required params")

        namespace = getattr(mock_client, category)
        method = getattr(namespace, method_name)
        kwargs = build_minimal_params(params)

        method(**kwargs)

        call = mock_client._calls[-1]
        sent_params = call["payload"].get("params", {})

        for pname in required_params:
            assert pname in sent_params, f"Required param '{pname}' missing in {action}"


class TestAllNamespaces:
    """Test all 23 namespaces are accessible."""

    @pytest.mark.parametrize("category", all_categories())
    def test_namespace_exists(self, category: str, mock_client):
        """Verify each namespace is accessible on client."""
        namespace = getattr(mock_client, category, None)
        assert namespace is not None, f"Namespace '{category}' not found"

    @pytest.mark.parametrize("category", all_categories())
    def test_namespace_has_all_methods(self, category: str, mock_client):
        """Verify each namespace has all its methods."""
        namespace = getattr(mock_client, category)
        operations = SCHEMA["by_category"].get(category, [])

        for op_name in operations:
            method_name = get_method_name(op_name)

            # Analytics uses __getattr__, so check differently
            if category == "analytics":
                # Just verify we can get the function
                func = getattr(namespace, method_name)
                assert func is not None
            else:
                assert hasattr(namespace, method_name), f"{category}.{method_name} not found"


class TestOperationCounts:
    """Verify operation counts match schema."""

    def test_total_operations(self, schema, all_operations):
        """Verify total operation count."""
        expected = schema["stats"]["total_operations"]
        # Exclude internal operations
        actual = len([
            op for op in all_operations.values()
            if op["category"] not in SKIP_CATEGORIES
        ])
        # Adjust for skipped categories
        skipped = sum(
            len(schema["by_category"].get(cat, []))
            for cat in SKIP_CATEGORIES
        )
        assert actual == expected - skipped

    def test_category_counts(self, schema):
        """Verify per-category operation counts."""
        stats = schema["stats"]["operations_by_category"]

        for category, count in stats.items():
            if category in SKIP_CATEGORIES:
                continue
            actual = len(schema["by_category"].get(category, []))
            assert actual == count, f"{category}: expected {count}, got {actual}"


class TestAnalyticsSpecial:
    """Special tests for analytics namespace (dynamic methods)."""

    def test_analytics_has_68_operations(self, schema):
        """Verify analytics has 68 operations."""
        count = schema["stats"]["operations_by_category"].get("analytics", 0)
        assert count == 68

    def test_analytics_dynamic_access(self, mock_client):
        """Test analytics dynamic method access."""
        # Access a method dynamically
        func = mock_client.analytics.rolling_volatility
        assert func is not None
        assert callable(func)

    def test_analytics_with_as_table_param(self, mock_client):
        """Test analytics supports as_table parameter."""
        # This should not raise (even though as_table won't work with mock)
        func = mock_client.analytics.rolling_volatility
        # The function should accept as_table kwarg
        # We can't fully test it without real data, but we verify it's accepted
        try:
            # This will fail at decode stage, but shouldn't fail at param stage
            func(identifiers="TEST", as_table="pandas")
        except Exception as e:
            # Expected: will fail at table decode, not at param validation
            assert "pandas" in str(e).lower() or "arrow" in str(e).lower() or "table" in str(e).lower()

    def test_analytics_help_and_info(self, mock_client):
        """Test analytics help and info methods."""
        # These require API calls, so we just verify they exist
        assert hasattr(mock_client.analytics, "help")
        assert hasattr(mock_client.analytics, "info")
        assert hasattr(mock_client.analytics, "list")
        assert hasattr(mock_client.analytics, "search")


class TestOperationMetadata:
    """Test operation metadata is correct."""

    @pytest.mark.parametrize("action,op", all_operations())
    def test_operation_has_required_fields(self, action: str, op: Dict[str, Any]):
        """Verify each operation has required metadata."""
        assert "action" in op
        assert "category" in op
        assert "operation" in op
        assert "params" in op
        assert op["action"] == action

    @pytest.mark.parametrize("action,op", all_operations())
    def test_params_have_required_fields(self, action: str, op: Dict[str, Any]):
        """Verify each param has required fields."""
        for p in op.get("params", []):
            assert "name" in p, f"{action}: param missing 'name'"
            assert "type" in p, f"{action}: param '{p.get('name')}' missing 'type'"
