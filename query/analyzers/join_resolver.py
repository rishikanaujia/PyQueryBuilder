# pyquerybuilder/query/analyzers/join_resolver.py
"""Resolver for join specifications in queries."""
from typing import Dict, Any, Optional


def resolve_join(join_spec, schema_registry):
    """Resolve a join specification to full join details.

    Args:
        join_spec: Join specification from query
        schema_registry: Schema information registry

    Returns:
        Resolved join dictionary or None if invalid
    """
    table = join_spec.get("table")
    if not table:
        return None

    # Check for alias in the table specification
    if " as " in table.lower():
        table_name, alias = table.lower().split(" as ", 1)
        table_name = table_name.strip()
        alias = alias.strip()
    elif " AS " in table:
        table_name, alias = table.split(" AS ", 1)
        table_name = table_name.strip()
        alias = alias.strip()
    else:
        table_name = table
        alias = None

    # Get actual table name from schema registry
    actual_table = table_name
    if table_name not in schema_registry.tables:
        # Look through all tables to find matching alias
        for t, info in schema_registry.tables.items():
            if info.get("alias") == table_name:
                actual_table = t
                break

    # If we couldn't find the table, return None
    if actual_table not in schema_registry.tables:
        return None

    # Use provided join condition or look up in registry
    condition = join_spec.get("condition")
    if not condition:
        # Try to find join path between the from_table and this table
        # This requires having the from_table information passed in
        join_paths = schema_registry.join_paths
        if actual_table in join_paths:
            condition = join_paths[actual_table].get("condition")

    # If we still don't have a condition, return None
    if not condition:
        return None

    return {
        "table": actual_table,
        "alias": alias or schema_registry.tables.get(actual_table, {}).get("alias"),
        "condition": condition,
        "type": join_spec.get("type", "INNER")
    }