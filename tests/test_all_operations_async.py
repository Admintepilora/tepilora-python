"""
Parametric async tests for all SDK operations.

Tests that each operation:
1. Is accessible via the correct namespace
2. Sends the correct action to the API
3. Handles required parameters correctly
"""

from typing import Any, Dict

import pytest

from conftest import build_minimal_params, _load_schema


# Load schema at module level for parametrize
SCHEMA = _load_schema()

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


class TestAllOperationsAsync:
    """Test all operations send correct actions using async client."""

    @pytest.mark.parametrize("action,op", all_operations())
    async def test_operation_sends_correct_action(self, action: str, op: Dict[str, Any], async_mock_client):
        """Verify each operation sends the correct action string."""
        if action in SPECIAL_OPERATIONS:
            pytest.skip(f"{action} uses special endpoint")

        category = op["category"]
        operation = op["operation"]
        method_name = get_method_name(operation)
        params = op.get("params", [])

        namespace = getattr(async_mock_client, category, None)
        assert namespace is not None, f"Namespace '{category}' not found on client"

        method = getattr(namespace, method_name, None)
        assert method is not None, f"Method '{method_name}' not found on {category}"

        kwargs = build_minimal_params(params)

        try:
            await method(**kwargs)
        except Exception as e:
            pytest.fail(f"Method {action} raised {type(e).__name__}: {e}")

        assert len(async_mock_client._calls) >= 1, f"No calls recorded for {action}"

        call = async_mock_client._calls[-1]
        sent_action = call["payload"].get("action")

        assert sent_action == action, f"Expected action '{action}', got '{sent_action}'"

    @pytest.mark.parametrize("action,op", all_operations())
    async def test_operation_includes_required_params(self, action: str, op: Dict[str, Any], async_mock_client):
        """Verify required params are included in request."""
        if action in SPECIAL_OPERATIONS:
            pytest.skip(f"{action} uses special endpoint")

        category = op["category"]
        operation = op["operation"]
        method_name = get_method_name(operation)
        params = op.get("params", [])
        required_params = [p["name"] for p in params if p.get("required") and p["name"] != "format"]

        if not required_params:
            pytest.skip("No required params")

        namespace = getattr(async_mock_client, category)
        method = getattr(namespace, method_name)
        kwargs = build_minimal_params(params)

        await method(**kwargs)

        call = async_mock_client._calls[-1]
        sent_params = call["payload"].get("params", {})

        for pname in required_params:
            assert pname in sent_params, f"Required param '{pname}' missing in {action}"


class TestAllNamespacesAsync:
    """Test all namespaces are accessible on async client."""

    @pytest.mark.parametrize("category", all_categories())
    def test_namespace_exists(self, category: str, async_mock_client):
        """Verify each namespace is accessible on async client."""
        namespace = getattr(async_mock_client, category, None)
        assert namespace is not None, f"Namespace '{category}' not found"

    @pytest.mark.parametrize("category", all_categories())
    def test_namespace_has_all_methods(self, category: str, async_mock_client):
        """Verify each namespace has all its methods."""
        namespace = getattr(async_mock_client, category)
        operations = SCHEMA["by_category"].get(category, [])

        for op_name in operations:
            method_name = get_method_name(op_name)

            if category == "analytics":
                func = getattr(namespace, method_name)
                assert func is not None
            else:
                assert hasattr(namespace, method_name), f"{category}.{method_name} not found"


class TestOperationCountsAsync:
    """Verify operation counts match schema."""

    def test_total_operations(self, schema, all_operations):
        """Verify total operation count."""
        expected = schema["stats"]["total_operations"]
        actual = len([op for op in all_operations.values() if op["category"] not in SKIP_CATEGORIES])
        skipped = sum(len(schema["by_category"].get(cat, [])) for cat in SKIP_CATEGORIES)
        assert actual == expected - skipped

    def test_category_counts(self, schema):
        """Verify per-category operation counts."""
        stats = schema["stats"]["operations_by_category"]

        for category, count in stats.items():
            if category in SKIP_CATEGORIES:
                continue
            actual = len(schema["by_category"].get(category, []))
            assert actual == count, f"{category}: expected {count}, got {actual}"


class TestAnalyticsSpecialAsync:
    """Special tests for async analytics namespace (dynamic methods)."""

    def test_analytics_has_68_operations(self, schema):
        """Verify analytics operation count."""
        count = schema["stats"]["operations_by_category"].get("analytics", 0)
        assert count == 68

    def test_analytics_dynamic_access(self, async_mock_client):
        """Test async analytics dynamic method access."""
        func = async_mock_client.analytics.rolling_volatility
        assert func is not None
        assert callable(func)

    async def test_analytics_with_as_table_param(self, async_mock_client):
        """Test async analytics supports as_table parameter."""
        func = async_mock_client.analytics.rolling_volatility
        try:
            await func(identifiers="TEST", as_table="pandas")
        except Exception as e:
            assert "pandas" in str(e).lower() or "arrow" in str(e).lower() or "table" in str(e).lower()

    async def test_analytics_help_and_info(self, async_mock_client):
        """Test async analytics list/info/help/search methods are awaitable."""
        listing = await async_mock_client.analytics.list()
        info = await async_mock_client.analytics.info("rolling_volatility")
        help_text = await async_mock_client.analytics.help("rolling_volatility")
        matches = await async_mock_client.analytics.search("rolling")

        assert isinstance(listing, dict)
        assert isinstance(info, dict)
        assert isinstance(help_text, str)
        assert isinstance(matches, list)


class TestOperationMetadataAsync:
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
