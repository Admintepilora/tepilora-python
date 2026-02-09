"""
TepiloraSDK Capabilities - Dynamic discovery of all SDK operations.

Usage:
    # Standalone function
    from Tepilora import capabilities
    capabilities()              # Print all capabilities
    capabilities("analytics")   # Print analytics namespace
    capabilities(search="vol")  # Search operations

    # Client method
    client = TepiloraClient(api_key="...")
    client.capabilities()       # Same API
"""

from __future__ import annotations

from functools import lru_cache
from typing import Any, Dict, List, Optional, Union

from ._schema import SCHEMA
from .version import __version__


@lru_cache(maxsize=1)
def _load_schema() -> Dict[str, Any]:
    """Load schema from package data."""
    return SCHEMA


def _count_by_category(operations: Dict[str, Any]) -> Dict[str, int]:
    """Count operations per category."""
    counts: Dict[str, int] = {}
    for op in operations.values():
        if op.get("internal"):
            continue
        cat = op["category"]
        counts[cat] = counts.get(cat, 0) + 1
    return counts


def _format_summary(schema: Dict[str, Any]) -> str:
    """Format complete SDK summary."""
    operations = schema["operations"]
    counts = _count_by_category(operations)

    total_ops = sum(counts.values())
    total_ns = len(counts)

    lines = [
        f"TepiloraSDK v{__version__} - {total_ops} operations in {total_ns} namespaces",
        "",
    ]

    # Sort by op count descending
    sorted_cats = sorted(counts.items(), key=lambda x: (-x[1], x[0]))

    for cat, count in sorted_cats:
        # Get operation names for this category
        ops = [
            op["operation"]
            for op in operations.values()
            if op["category"] == cat and not op.get("internal")
        ]
        ops_str = ", ".join(sorted(ops)[:5])
        if len(ops) > 5:
            ops_str += f", ... (+{len(ops) - 5} more)"

        lines.append(f"  {cat} ({count}): {ops_str}")

    return "\n".join(lines)


def _format_namespace(schema: Dict[str, Any], namespace: str) -> str:
    """Format detailed info for a specific namespace."""
    operations = schema["operations"]

    # Filter operations for this namespace
    ns_ops = [
        op for op in operations.values()
        if op["category"] == namespace and not op.get("internal")
    ]

    if not ns_ops:
        available = sorted(set(
            op["category"] for op in operations.values() if not op.get("internal")
        ))
        return f"Namespace '{namespace}' not found.\nAvailable: {', '.join(available)}"

    lines = [
        f"{namespace} - {len(ns_ops)} operations",
        "",
    ]

    # Sort by operation name
    for op in sorted(ns_ops, key=lambda x: x["operation"]):
        name = op["operation"]
        summary = op.get("summary", "")
        params = op.get("params", [])

        # Build signature
        required = [p["name"] for p in params if p.get("required")]
        optional = [p["name"] for p in params if not p.get("required")]

        sig_parts = []
        if required:
            sig_parts.append(", ".join(required))
        if optional:
            opt_str = ", ".join(f"{p}=..." for p in optional[:3])
            if len(optional) > 3:
                opt_str += f", +{len(optional) - 3} more"
            sig_parts.append(opt_str)

        sig = ", ".join(sig_parts) if sig_parts else ""

        # Tree-style output
        prefix = "  "
        line = f"{prefix}{name}({sig})"
        if summary:
            line += f"  # {summary}"
        lines.append(line)

    return "\n".join(lines)


def _format_search(schema: Dict[str, Any], query: str) -> str:
    """Search operations by name or description."""
    operations = schema["operations"]
    query_lower = query.lower()

    matches = []
    for action, op in operations.items():
        if op.get("internal"):
            continue

        # Search in action, operation, summary, description
        searchable = " ".join([
            action,
            op.get("operation", ""),
            op.get("summary", ""),
            op.get("description", ""),
        ]).lower()

        if query_lower in searchable:
            matches.append((action, op))

    if not matches:
        return f"No operations matching '{query}'"

    lines = [
        f"Found {len(matches)} operation(s) matching '{query}':",
        "",
    ]

    for action, op in sorted(matches, key=lambda x: x[0]):
        summary = op.get("summary", "")
        line = f"  {action}"
        if summary:
            line += f" - {summary}"
        lines.append(line)

    return "\n".join(lines)


def _format_operation(schema: Dict[str, Any], action: str) -> str:
    """Format detailed info for a specific operation."""
    operations = schema["operations"]

    op = operations.get(action)
    if not op:
        # Try to find partial match
        matches = [a for a in operations if action in a]
        if matches:
            return f"Operation '{action}' not found. Did you mean: {', '.join(matches[:5])}"
        return f"Operation '{action}' not found."

    lines = [
        f"{action}",
        f"  Category: {op['category']}",
        f"  Credits: {op.get('credits', 1)}",
    ]

    if op.get("summary"):
        lines.append(f"  Summary: {op['summary']}")

    if op.get("description"):
        lines.append(f"  Description: {op['description']}")

    params = op.get("params", [])
    if params:
        lines.append("")
        lines.append("  Parameters:")
        for p in params:
            req = " (required)" if p.get("required") else ""
            default = f" = {p['default']}" if "default" in p else ""
            desc = f" - {p.get('description', '')}" if p.get("description") else ""
            lines.append(f"    {p['name']}: {p.get('type', 'any')}{default}{req}{desc}")

    if op.get("tags"):
        lines.append(f"  Tags: {', '.join(op['tags'])}")

    if op.get("deprecated"):
        lines.append("  [DEPRECATED]")

    return "\n".join(lines)


def capabilities(
    namespace_or_action: Optional[str] = None,
    *,
    search: Optional[str] = None,
    format: str = "text",
) -> Union[str, Dict[str, Any], None]:
    """
    Discover TepiloraSDK capabilities.

    Args:
        namespace_or_action: Namespace name (e.g., "analytics") or full action
                            (e.g., "analytics.rolling_volatility")
        search: Search query to find operations by name/description
        format: Output format - "text" (default), "dict", or "print"

    Returns:
        str if format="text", dict if format="dict", None if format="print"

    Examples:
        # Print all capabilities
        capabilities()

        # Explore a namespace
        capabilities("analytics")

        # Get details on specific operation
        capabilities("analytics.rolling_volatility")

        # Search operations
        capabilities(search="volatility")

        # Get raw data
        data = capabilities(format="dict")
    """
    schema = _load_schema()

    # Raw dict output
    if format == "dict":
        if namespace_or_action:
            # Return filtered data
            operations = schema["operations"]
            if "." in namespace_or_action:
                # Specific operation
                return operations.get(namespace_or_action)
            else:
                # Namespace
                return {
                    action: op for action, op in operations.items()
                    if op["category"] == namespace_or_action and not op.get("internal")
                }
        return schema

    # Text output
    if search:
        result = _format_search(schema, search)
    elif namespace_or_action:
        if "." in namespace_or_action:
            # Full action like "analytics.rolling_volatility"
            result = _format_operation(schema, namespace_or_action)
        else:
            # Namespace like "analytics"
            result = _format_namespace(schema, namespace_or_action)
    else:
        result = _format_summary(schema)

    if format == "print":
        print(result)
        return None

    return result


# For convenience when used as client method
def _client_capabilities(
    self: Any,
    namespace_or_action: Optional[str] = None,
    *,
    search: Optional[str] = None,
    format: str = "print",
) -> Union[str, Dict[str, Any], None]:
    """Client method wrapper - defaults to print format."""
    return capabilities(namespace_or_action, search=search, format=format)


# Quick accessors
def list_namespaces() -> List[str]:
    """List all available namespaces."""
    schema = _load_schema()
    operations = schema["operations"]
    return sorted(set(
        op["category"] for op in operations.values() if not op.get("internal")
    ))


def list_operations(namespace: Optional[str] = None) -> List[str]:
    """List all operations, optionally filtered by namespace."""
    schema = _load_schema()
    operations = schema["operations"]

    result = []
    for action, op in operations.items():
        if op.get("internal"):
            continue
        if namespace and op["category"] != namespace:
            continue
        result.append(action)

    return sorted(result)


def get_operation_info(action: str) -> Optional[Dict[str, Any]]:
    """Get detailed info for a specific operation."""
    schema = _load_schema()
    return schema["operations"].get(action)
