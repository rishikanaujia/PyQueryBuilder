# pyquerybuilder/query/analyzers/join_analyzer.py
"""Analyzer for join conditions in queries."""
from typing import Dict, List, Any, Set

from .join_path_finder import find_join_path
from .join_resolver import resolve_join


def analyze_joins(from_table, joins, required_tables, schema_registry):
    """Analyze join specifications and resolve requirements."""
    resolved_joins = []
    joined_tables = {from_table["table"]}

    # Process explicit joins
    for join in joins:
        resolved_join = resolve_join(join, schema_registry)
        if resolved_join:
            resolved_joins.append(resolved_join)
            joined_tables.add(resolved_join["table"])

    # Add implicit joins for required tables
    for table in required_tables:
        if table not in joined_tables:
            implicit_join = find_join_path(
                from_table["table"], table, schema_registry
            )
            if implicit_join:
                resolved_joins.append(implicit_join)
                joined_tables.add(table)

    return {
        "resolved_joins": resolved_joins,
        "joined_tables": list(joined_tables)
    }