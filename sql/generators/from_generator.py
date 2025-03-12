# pyquerybuilder/sql/generators/from_generator.py
"""Generator for FROM clause in SQL queries."""
from typing import Dict, Any, Union


def generate_from(from_spec: Union[Dict, Any]) -> str:
    """Generate a FROM clause.

    Args:
        from_spec: Table specification or subquery object

    Returns:
        FROM clause string
    """
    # Handle subquery objects
    if hasattr(from_spec, 'get_sql'):
        return f"FROM {from_spec.get_sql()}"

    # Handle regular table specifications
    if isinstance(from_spec, dict):
        table_name = from_spec.get("table")
        alias = from_spec.get("alias")

        if alias:
            return f"FROM {table_name} AS {alias}"
        else:
            return f"FROM {table_name}"

    # Fallback for direct string input
    return f"FROM {from_spec}"

# # pyquerybuilder/sql/generators/from_generator.py
# """Generator for FROM clause in SQL queries."""
# from typing import Dict, Any
#
#
# def generate_from(from_spec):
#     """Generate a FROM clause.
#
#     Args:
#         from_spec: Dictionary with table name and alias
#
#     Returns:
#         FROM clause string
#     """
#     table_name = from_spec["table"]
#     alias = from_spec.get("alias")
#
#     if alias:
#         return f"FROM {table_name} AS {alias}"
#     else:
#         return f"FROM {table_name}"