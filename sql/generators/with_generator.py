# pyquerybuilder/sql/generators/with_generator.py
"""Generator for WITH clauses in SQL queries."""
from typing import List, Dict, Any


def generate_with(ctes):
    """Generate a WITH clause for Common Table Expressions.

    Args:
        ctes: List of CommonTableExpression objects

    Returns:
        WITH clause string
    """
    if not ctes:
        return ""

    # Check if any CTEs are recursive
    recursive = any(cte.recursive for cte in ctes)
    recursive_keyword = "RECURSIVE " if recursive else ""

    # Generate CTE definitions
    cte_parts = []
    for cte in ctes:
        cte_parts.append(cte.get_sql())

    # Build the WITH clause
    return f"WITH {recursive_keyword}{', '.join(cte_parts)}"