# pyquerybuilder/schema/join_builder.py
"""Functions for building join paths between tables."""
from typing import Dict, Any

from schema.alias_generator import generate_alias


def add_join_path(registry, source_table, target_table,
                  source_column, target_column):
    """Add a join path between two tables.

    Args:
        registry: Schema registry instance
        source_table: Name of the source table
        target_table: Name of the target table
        source_column: Join column in source table
        target_column: Join column in target table
    """
    # Create alias for target table
    alias = generate_alias(target_table)

    # Build the join path
    join_path = {
        "table": target_table,
        "alias": alias,
        "condition": f"{source_table}.{source_column} = "
                     f"{alias}.{target_column}"
    }

    # Store in registry
    if source_table not in registry.join_paths:
        registry.join_paths[source_table] = {}

    registry.join_paths[source_table][target_table] = join_path