# pyquerybuilder/sql/generators/group_generator.py
"""Generator for GROUP BY clause in SQL queries."""
from typing import List, Any


def generate_group_by(group_fields):
    """Generate a GROUP BY clause.

    Args:
        group_fields: List of fields to group by

    Returns:
        GROUP BY clause string
    """
    if not group_fields:
        return ""

    group_items = []

    for field in group_fields:
        # Handle function objects
        if hasattr(field, 'get_sql'):
            group_items.append(field.get_sql())
        else:
            group_items.append(str(field))

    return "GROUP BY " + ", ".join(group_items)