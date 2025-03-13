# pyquerybuilder/core/cte.py
"""Support for Common Table Expressions (WITH clauses) in PyQueryBuilder."""
from typing import Dict, List, Optional, Any


class CommonTableExpression:
    """Represents a Common Table Expression (CTE) for use in WITH clauses."""

    def __init__(self, query, name: str, recursive: bool = False):
        """Initialize a CTE.

        Args:
            query: QueryBuilder instance to use as the CTE
            name: Name for this CTE
            recursive: Whether this is a recursive CTE
        """
        self.query = query
        self.name = name
        self.recursive = recursive

    def get_sql(self) -> str:
        """Generate SQL for this CTE.

        Returns:
            SQL string for the CTE definition
        """
        # Get the SQL for the query
        query_sql, _ = self.query.build()

        # Return formatted CTE
        return f"{self.name} AS ({query_sql})"