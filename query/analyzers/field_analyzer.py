# pyquerybuilder/query/analyzers/field_analyzer.py
"""Analyzer for field references in queries."""
from typing import Dict, List, Any, Set

from .field_resolver import resolve_field


def analyze_fields(select_fields, schema_registry):
    """Analyze field references and determine requirements.

    Args:
        select_fields: List of fields to analyze
        schema_registry: Schema information registry

    Returns:
        Dictionary with field information and requirements
    """
    table_requirements = set()
    field_info = []

    for field in select_fields:
        # Handle function objects
        if hasattr(field, 'get_sql'):
            # Add field as is
            field_info.append({
                "original": field,
                "is_function": True,
                "field_name": None,
                "alias": getattr(field, 'alias', None),
                "table": None,
                "column": None
            })

            # Extract table requirements from function arguments
            _extract_function_dependencies(field, table_requirements, schema_registry)
            continue

        # Check for alias in the field
        if isinstance(field, str):
            if " as " in field.lower():
                field_expr, alias = field.split(" as ", 1)
                field_name = field_expr.strip()
                alias = alias.strip()
            elif " AS " in field:
                field_expr, alias = field.split(" AS ", 1)
                field_name = field_expr.strip()
                alias = alias.strip()
            else:
                field_name = field
                alias = None

            # Analyze the field name
            table, column = resolve_field(field_name, schema_registry)

            if table:
                table_requirements.add(table)

            field_info.append({
                "original": field,
                "field_name": field_name,
                "alias": alias,
                "table": table,
                "column": column,
                "is_function": False
            })

    return {
        "field_info": field_info,
        "required_tables": list(table_requirements)
    }


def _extract_function_dependencies(func_obj, table_requirements, schema_registry):
    """Extract table dependencies from a function object.

    Args:
        func_obj: Function object to analyze
        table_requirements: Set to add table requirements to
        schema_registry: Schema information registry
    """
    # Check each argument of the function
    for arg in getattr(func_obj, 'args', []):
        # Recursive analysis for nested functions
        if hasattr(arg, 'get_sql'):
            _extract_function_dependencies(arg, table_requirements, schema_registry)
        # String arguments might be field references
        elif isinstance(arg, str) and not arg.startswith("'") and not arg.isdigit():
            # Skip literal strings and numbers
            if "." in arg:
                # This looks like a table.column reference
                parts = arg.split(".", 1)
                table_alias = parts[0].strip()

                # Resolve the table alias to an actual table
                from .alias_resolver import resolve_alias
                table = resolve_alias(table_alias, schema_registry)
                if table:
                    table_requirements.add(table)
            else:
                # Unqualified column name - try to find in schema
                for table, columns in schema_registry.columns.items():
                    if arg in columns:
                        table_requirements.add(table)
                        break


# # pyquerybuilder/query/analyzers/field_analyzer.py
# """Analyzer for field references in queries."""
# from typing import Dict, List, Any, Set
#
# from query.analyzers.field_resolver import resolve_field
#
#
# #from .field_resolver import resolve_field  # Import the correct function
#
#
# def analyze_fields(select_fields, schema_registry):
#     """Analyze field references and determine requirements."""
#     table_requirements = set()
#     field_info = []
#
#     for field in select_fields:
#         # Check for alias in the field
#         if " as " in field.lower():
#             field_expr, alias = field.split(" as ", 1)
#             field_name = field_expr.strip()
#         elif " AS " in field:
#             field_expr, alias = field.split(" AS ", 1)
#             field_name = field_expr.strip()
#         else:
#             field_name = field
#             alias = None
#
#         # Analyze the field name - use the imported function
#         table, column = resolve_field(field_name, schema_registry)
#
#         if table:
#             table_requirements.add(table)
#
#         field_info.append({
#             "original": field,
#             "field_name": field_name,
#             "alias": alias,
#             "table": table,
#             "column": column
#         })
#
#     return {
#         "field_info": field_info,
#         "required_tables": list(table_requirements)
#     }
#
#
# def resolve_alias(alias, schema_registry):
#     """Resolve a table alias to actual table name.
#
#     Args:
#         alias: Table alias to resolve
#         schema_registry: Schema information registry
#
#     Returns:
#         Actual table name or None if not found
#     """
#     # Check if this is an actual table name
#     if alias in schema_registry.tables:
#         return alias
#
#     # Look up in alias mappings
#     for table, table_info in schema_registry.tables.items():
#         if table_info.get("alias") == alias:
#             return table
#
#     # Alias not found
#     return None
