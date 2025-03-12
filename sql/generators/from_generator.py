# pyquerybuilder/sql/generators/from_generator.py
"""Generator for FROM clause in SQL queries."""
from typing import Dict, Any


def generate_from(from_spec):
    """Generate a FROM clause.

    Args:
        from_spec: Dictionary with table name and alias

    Returns:
        FROM clause string
    """
    table_name = from_spec["table"]
    alias = from_spec.get("alias")

    if alias:
        return f"FROM {table_name} AS {alias}"
    else:
        return f"FROM {table_name}"