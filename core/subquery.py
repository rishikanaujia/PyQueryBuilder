# pyquerybuilder/core/subquery.py
"""Support for subqueries in PyQueryBuilder."""
from typing import Optional


class Subquery:
    """Wrapper for a query to be used as a subquery."""

    def __init__(self, query_builder, alias: Optional[str] = None):
        """Initialize with a query builder instance.

        Args:
            query_builder: QueryBuilder instance
            alias: Optional alias for the subquery
        """
        self.query_builder = query_builder
        self.alias = alias

    def as_(self, alias: str) -> "Subquery":
        """Set the alias for this subquery.

        Args:
            alias: Subquery alias

        Returns:
            Self for method chaining
        """
        self.alias = alias
        return self

    def get_sql(self) -> str:
        """Generate SQL for this subquery.

        Returns:
            SQL string with proper alias
        """
        # Get the SQL from the wrapped query builder
        sql, _ = self.query_builder.build()

        # Add parentheses around the subquery
        subquery_sql = f"({sql})"

        # Add alias if provided
        if self.alias:
            return f"{subquery_sql} AS {self.alias}"

        return subquery_sql