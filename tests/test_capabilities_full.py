"""Comprehensive capability coverage tests."""

from Tepilora import capabilities, get_operation_info, list_namespaces, list_operations


def _schema():
    return capabilities(format="dict")


def _operations(schema):
    return {action: op for action, op in schema["operations"].items() if not op.get("internal")}


def _group_by_namespace(ops):
    grouped = {}
    for action, op in ops.items():
        ns = op["category"]
        grouped.setdefault(ns, []).append((action, op))
    return grouped


def test_schema_and_helpers_are_consistent():
    schema = _schema()
    ops = _operations(schema)
    assert ops

    # Validate stats and categories
    stats = schema.get("stats", {})
    namespaces = sorted({op["category"] for op in ops.values()})
    assert stats.get("total_operations") == len(ops)
    assert stats.get("categories") == len(namespaces)
    assert sorted(schema.get("categories", [])) == namespaces

    # Validate per-namespace stats
    ops_by_category = stats.get("operations_by_category", {})
    grouped = _group_by_namespace(ops)
    for ns, ns_ops in grouped.items():
        assert ops_by_category.get(ns) == len(ns_ops)

    # Helpers match schema
    assert list_namespaces() == namespaces
    assert list_operations() == sorted(ops)
    for ns in namespaces:
        ns_actions = sorted(a for a, op in ops.items() if op["category"] == ns)
        assert list_operations(ns) == ns_actions


def test_namespace_text_and_dict_outputs_cover_all_operations():
    schema = _schema()
    ops = _operations(schema)
    grouped = _group_by_namespace(ops)

    for ns, ns_ops in grouped.items():
        ns_text = capabilities(ns)
        assert f"{ns} - {len(ns_ops)} operations" in ns_text
        for _, op in ns_ops:
            assert f"  {op['operation']}(" in ns_text

        ns_dict = capabilities(ns, format="dict")
        assert isinstance(ns_dict, dict)
        assert sorted(ns_dict) == sorted(a for a, op in ns_ops)


def test_operation_detail_and_search_outputs_are_complete():
    schema = _schema()
    ops = _operations(schema)

    for action, op in ops.items():
        # Detail view
        detail = capabilities(action)
        assert detail.splitlines()[0] == action
        assert f"Category: {op['category']}" in detail
        assert f"Summary: {op['summary']}" in detail

        params = op.get("params", [])
        if params:
            assert "Parameters:" in detail
            for p in params:
                assert f"{p['name']}:" in detail

        # Dict view
        op_dict = capabilities(action, format="dict")
        assert op_dict == op

        # Helper
        assert get_operation_info(action) == op

        # Search should find the action by full name
        search = capabilities(search=action)
        assert action in search
