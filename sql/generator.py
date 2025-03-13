# pyquerybuilder/sql/generator.py
"""Generator for producing SQL from analyzed queries."""

from .generators.select_generator import generate_select
from .generators.from_generator import generate_from
from .generators.join_generator import generate_joins
from .generators.where_generator import generate_where
from .generators.group_generator import generate_group_by
from .generators.order_generator import generate_order_by


class SQLGenerator:
    """Generates SQL queries from analyzed components."""

    def __init__(self, dialect="snowflake"):
        """Initialize with SQL dialect."""
        self.dialect = dialect

    def generate(self, analyzed_query):
        """Generate SQL from analyzed query components."""
        params = {}

        # Generate each clause
        select_clause = generate_select(
            analyzed_query.get("select_fields", [])
        )

        # Handle either from_table or from_subquery
        if analyzed_query.get("from_subquery"):
            from_clause = generate_from(analyzed_query["from_subquery"])
        else:
            from_clause = generate_from(analyzed_query.get("from_table", {}))

        join_clause = generate_joins(
            analyzed_query.get("joins", [])
        )

        where_clause, where_params = generate_where(
            analyzed_query.get("where_conditions", []),
            analyzed_query.get("where_groups", [])  # Added this line
        )
        params.update(where_params)

        group_clause = generate_group_by(
            analyzed_query.get("group_by", [])
        )

        order_clause = generate_order_by(
            analyzed_query.get("order_by", [])
        )

        # Build final SQL
        sql_parts = [
            select_clause,
            from_clause,
            join_clause,
            where_clause,
            group_clause,
            order_clause
        ]

        sql = " ".join(part for part in sql_parts if part)

        return sql, params

# # pyquerybuilder/sql/generator.py
# """Generator for producing SQL from analyzed queries."""
# from typing import Dict, List, Tuple, Any
#
# from .generators.select_generator import generate_select
# from .generators.from_generator import generate_from
# from .generators.join_generator import generate_joins
# from .generators.where_generator import generate_where
# from .generators.order_generator import generate_order_by
#
#
# class SQLGenerator:
#     """Generates SQL queries from analyzed components."""
#
#     def __init__(self, dialect="snowflake"):
#         """Initialize with SQL dialect."""
#         self.dialect = dialect
#
#     def generate(self, analyzed_query):
#         """Generate SQL from analyzed query components.
#
#         Args:
#             analyzed_query: Dictionary of analyzed components
#
#         Returns:
#             Tuple of (sql_string, parameters)
#         """
#         params = {}
#
#         # Generate each clause
#         select_clause = generate_select(
#             analyzed_query.get("select_fields", [])
#         )
#
#         from_clause = generate_from(
#             analyzed_query.get("from_table", {})
#         )
#
#         join_clause = generate_joins(
#             analyzed_query.get("joins", [])
#         )
#
#         where_clause, where_params = generate_where(
#             analyzed_query.get("where_conditions", [])
#         )
#         params.update(where_params)
#
#         order_clause = generate_order_by(
#             analyzed_query.get("order_by", [])
#         )
#
#         # Combine all clauses
#         sql_parts = [
#             select_clause,
#             from_clause,
#             join_clause,
#             where_clause,
#             order_clause
#         ]
#
#         # Build final SQL string
#         sql = " ".join(part for part in sql_parts if part)
#
#         return sql, params
