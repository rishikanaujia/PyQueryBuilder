# pyquerybuilder/sql/generators/join_generator.py
"""Generator for JOIN clauses in SQL queries."""
from typing import List, Dict, Any


def generate_joins(joins):
    """Generate JOIN clauses from join specifications."""
    if not joins:
        return ""

    join_clauses = []

    for join in joins:
        table = join["table"]
        alias = join.get("alias")
        condition = join["condition"]
        join_type = join.get("type", "INNER").upper()

        # Handle subquery objects
        if hasattr(table, 'get_sql'):
            table_sql = table.get_sql()
        else:
            table_sql = str(table)

        join_str = f"{join_type} JOIN {table_sql}"
        if alias and not hasattr(table, 'get_sql'):
            join_str += f" AS {alias}"
        join_str += f" ON {condition}"

        join_clauses.append(join_str)

    return " ".join(join_clauses)


# # pyquerybuilder/sql/generators/join_generator.py
# """Generator for JOIN clauses in SQL queries."""
# from typing import List, Dict, Any
#
#
# def generate_joins(joins):
#     """Generate JOIN clauses from join specifications.
#
#     Args:
#         joins: List of join dictionaries
#
#     Returns:
#         String with all JOIN clauses
#     """
#     if not joins:
#         return ""
#
#     join_clauses = []
#
#     for join in joins:
#         table = join["table"]
#         alias = join.get("alias")
#         condition = join["condition"]
#         join_type = join.get("type", "INNER").upper()
#
#         join_str = f"{join_type} JOIN {table}"
#         if alias:
#             join_str += f" AS {alias}"
#         join_str += f" ON {condition}"
#
#         join_clauses.append(join_str)
#
#     return " ".join(join_clauses)