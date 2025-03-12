# pyquerybuilder/sql/generators/where_generator.py
"""Generator for WHERE clause in SQL queries."""
from typing import List, Dict, Any, Tuple


def generate_where(conditions, param_start_idx=0):
    """Generate a WHERE clause from conditions.

    Args:
        conditions: List of condition dictionaries
        param_start_idx: Starting index for parameters

    Returns:
        Tuple of (WHERE clause, parameters dict)
    """
    if not conditions:
        return "", {}

    where_parts = []
    params = {}
    param_idx = param_start_idx

    for condition in conditions:
        field = condition["field"]
        operator = condition["operator"]
        value = condition["value"]

        # Parameter name
        param_name = f"p{param_idx}"
        param_idx += 1

        # Handle the condition based on operator
        if operator.upper() in ("IS NULL", "IS NOT NULL"):
            where_parts.append(f"{field} {operator}")
        else:
            where_parts.append(f"{field} {operator} :{param_name}")
            params[param_name] = value

    where_clause = "WHERE " + " AND ".join(where_parts)
    return where_clause, params