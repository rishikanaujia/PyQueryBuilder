# pyquerybuilder/sql/generators/select_generator.py
"""Generator for SELECT clause in SQL queries."""
from typing import List, Dict, Any

# pyquerybuilder/sql/generators/select_generator.py
"""Generator for SELECT clause in SQL queries."""
from typing import List, Dict, Any


def generate_select(fields):
    """Generate a SELECT clause from field specifications."""
    if not fields:
        return "SELECT *"

    select_items = []

    for field in fields:
        # Handle function objects
        if hasattr(field, 'get_sql'):
            select_items.append(field.get_sql())
        # Handle strings with aliases
        elif isinstance(field, str) and (" as " in field.lower() or " AS " in field):
            select_items.append(field)
        # Handle plain strings
        elif isinstance(field, str):
            if "." in field:
                select_items.append(field)
            else:
                select_items.append(field)

    return "SELECT " + ", ".join(select_items)



# def generate_select(fields):
#     """Generate a SELECT clause from field specifications.
#
#     Args:
#         fields: List of fields to include
#
#     Returns:
#         SELECT clause string
#     """
#     if not fields:
#         return "SELECT *"
#
#     select_items = []
#
#     for field in fields:
#         # Handle aliases in field specification
#         if " as " in field.lower() or " AS " in field:
#             select_items.append(field)
#         else:
#             # If no alias and contains dot, use as is
#             if "." in field:
#                 select_items.append(field)
#             else:
#                 # Otherwise, it's an unqualified field name
#                 select_items.append(field)
#
#     return "SELECT " + ", ".join(select_items)