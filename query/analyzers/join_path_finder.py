# pyquerybuilder/query/analyzers/join_path_finder.py
"""Functions for finding join paths between tables."""
from typing import Dict, Optional


def find_join_path(source_table, target_table, schema_registry):
    """Find a join path between two tables.

    Args:
        source_table: Source table name
        target_table: Target table name
        schema_registry: Schema information registry

    Returns:
        Join dictionary or None if no path found
    """
    # Direct path from source to target
    if source_table in schema_registry.join_paths:
        if target_table in schema_registry.join_paths[source_table]:
            return schema_registry.join_paths[source_table][target_table]

    # Direct path from target to source
    if target_table in schema_registry.join_paths:
        if source_table in schema_registry.join_paths[target_table]:
            join_info = schema_registry.join_paths[target_table][source_table]
            # Swap the join direction
            return {
                "table": target_table,
                "alias": join_info.get("alias"),
                "condition": join_info["condition"],
                "type": "INNER"  # Default to inner join for implicit joins
            }

    # Path not found
    return None