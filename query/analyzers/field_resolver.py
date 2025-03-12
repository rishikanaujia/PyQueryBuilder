# pyquerybuilder/query/analyzers/field_resolver.py
"""Resolver for field references in queries."""
from typing import Dict, Tuple, Optional
from .alias_resolver import resolve_alias


def resolve_field(field_name, schema_registry):
    """Resolve a field reference to table and column.

    Args:
        field_name: Field name to resolve
        schema_registry: Schema information registry

    Returns:
        Tuple of (table_name, column_name)
    """
    # Check if field contains table qualifier
    if "." in field_name:
        parts = field_name.split(".", 1)
        table_alias = parts[0].strip()
        column_name = parts[1].strip()

        # Resolve alias to actual table
        table_name = resolve_alias(table_alias, schema_registry)
        if table_name:
            # Verify column exists in table
            if table_name in schema_registry.columns:
                if column_name in schema_registry.columns[table_name]:
                    return table_name, column_name

        # If we can't resolve the table or column doesn't exist,
        # return as is - the SQL might still be valid
        return table_alias, column_name

    # Unqualified field name - search in all tables
    for table_name, columns in schema_registry.columns.items():
        if field_name in columns:
            return table_name, field_name

    # Field not found in schema, return as is
    return None, field_name