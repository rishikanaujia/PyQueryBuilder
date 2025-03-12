# pyquerybuilder/sql/functions/base_function.py
"""Base class for SQL functions in PyQueryBuilder."""
from typing import Any, List, Optional


class Function:
    """Base class for all SQL functions."""

    def __init__(self, name: str, *args, alias: Optional[str] = None):
        """Initialize SQL function.

        Args:
            name: Function name (e.g., SUM, COUNT)
            *args: Function arguments
            alias: Optional alias for the function result
        """
        self.name = name
        self.args = args
        self.alias = alias

    def as_(self, alias: str) -> "Function":
        """Set alias for the function result.

        Args:
            alias: Alias name

        Returns:
            Self for method chaining
        """
        self.alias = alias
        return self

    def get_sql(self) -> str:
        """Generate SQL for this function.

        Returns:
            SQL string representation
        """
        args_sql = ", ".join(
            arg.get_sql() if hasattr(arg, "get_sql") else str(arg)
            for arg in self.args
        )

        function_sql = f"{self.name}({args_sql})"

        if self.alias:
            return f"{function_sql} AS {self.alias}"

        return function_sql