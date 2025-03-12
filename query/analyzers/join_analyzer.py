# pyquerybuilder/query/analyzers/join_analyzer.py
"""Analyzer for join conditions in queries."""
from typing import Dict, List, Any, Set

from .alias_resolver import resolve_alias


def analyze_joins(from_table, joins, required_tables, schema_registry):
    """Analyze join specifications and resolve requirements.

    Args:
        from_table: Main table information
        joins: List of join specifications
        required_tables: Tables required by fields
        schema_registry: Schema information registry

    Returns:
        Dictionary with resolved join information
    """
    resolved_joins = []
    joined_tables = {from_table["table"]}

    # Process explicit joins
    for join in joins:
        resolved_join = _resolve_join(join, schema_registry)
        if resolved_join:
            resolved_joins.append(resolved_join)
            joined_tables.add(resolved_join["table"])

    # Add implicit joins for required tables
    for table in required_tables:
        if table not in joined_tables:
            implicit_join = _find_join_path(
                from_table["table"], table, schema_registry
            )
            if implicit_join:
                resolved_joins.append(implicit_join)
                joined_tables.add(table)

    return {
        "resolved_joins": resolved_joins,
        "joined_tables": list(joined_tables)
    }


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

    # Get actual table name (in case table_name is an alias)
    actual_table = table_name
    if table_name not in schema_registry.tables:
        # Try to resolve as an alias
        for t, info in schema_registry.tables.items():
            if info.get("alias") == table_name:
                actual_table = t
                break

    # Use provided join condition or look up in schema
    condition = join_spec.get("condition")
    if not condition:
        # Try to find join path in registry
        join_info = schema_registry.join_paths.get(actual_table)
        if join_info and join_info.get("condition"):
            condition = join_info["condition"]

    return {
        "table": actual_table,
        "alias": alias or schema_registry.tables.get(actual_table, {}).get("alias"),
        "condition": condition,
        "type": join_spec.get("type", "INNER")
    }
