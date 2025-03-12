# pyquerybuilder/query/analyzers/alias_resolver.py
"""Resolver for table aliases in queries."""
from typing import Optional


def resolve_alias(alias, schema_registry):
    """Resolve a table alias to actual table name.

    Args:
        alias: Table alias to resolve
        schema_registry: Schema information registry

    Returns:
        Actual table name or None if not found
    """
    # Strip any whitespace from the alias
    alias = alias.strip()

    # Check if this is an actual table name
    if alias in schema_registry.tables:
        return alias

    # Look up in alias mappings
    alias_map = getattr(schema_registry, 'alias_map', {})
    if alias in alias_map:
        return alias_map[alias]

    # Check aliases in table info
    for table_name, table_info in schema_registry.tables.items():
        if table_info.get("alias") == alias:
            return table_name

    # Alias not found
    return None