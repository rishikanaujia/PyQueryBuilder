# pyquerybuilder/query/analyzers/field_analyzer.py
"""Analyzer for field references in queries."""
from typing import Dict, List, Any, Set

from query.analyzers.field_resolver import resolve_field


#from .field_resolver import resolve_field  # Import the correct function


def analyze_fields(select_fields, schema_registry):
    """Analyze field references and determine requirements."""
    table_requirements = set()
    field_info = []

    for field in select_fields:
        # Check for alias in the field
        if " as " in field.lower():
            field_expr, alias = field.split(" as ", 1)
            field_name = field_expr.strip()
        elif " AS " in field:
            field_expr, alias = field.split(" AS ", 1)
            field_name = field_expr.strip()
        else:
            field_name = field
            alias = None

        # Analyze the field name - use the imported function
        table, column = resolve_field(field_name, schema_registry)

        if table:
            table_requirements.add(table)

        field_info.append({
            "original": field,
            "field_name": field_name,
            "alias": alias,
            "table": table,
            "column": column
        })

    return {
        "field_info": field_info,
        "required_tables": list(table_requirements)
    }


def resolve_alias(alias, schema_registry):
    """Resolve a table alias to actual table name.

    Args:
        alias: Table alias to resolve
        schema_registry: Schema information registry

    Returns:
        Actual table name or None if not found
    """
    # Check if this is an actual table name
    if alias in schema_registry.tables:
        return alias

    # Look up in alias mappings
    for table, table_info in schema_registry.tables.items():
        if table_info.get("alias") == alias:
            return table

    # Alias not found
    return None
