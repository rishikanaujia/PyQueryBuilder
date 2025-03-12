# pyquerybuilder/sql/functions/aggregate_functions.py
"""Aggregate SQL functions for PyQueryBuilder."""
from typing import Any, Optional

from .base_function import Function


class AggregateFunction(Function):
    """Base class for aggregate functions."""

    def __init__(self, name: str, field: Any, alias: Optional[str] = None):
        """Initialize aggregate function.

        Args:
            name: Function name (e.g., SUM, COUNT)
            field: Field to aggregate
            alias: Optional alias for the result
        """
        super().__init__(name, field, alias=alias)
        self._distinct = False

    def distinct(self) -> "AggregateFunction":
        """Apply DISTINCT to the function argument.

        Returns:
            Self for method chaining
        """
        self._distinct = True
        return self

    def get_sql(self) -> str:
        """Generate SQL with optional DISTINCT.

        Returns:
            SQL string representation
        """
        arg = self.args[0]
        arg_sql = arg.get_sql() if hasattr(arg, "get_sql") else str(arg)

        if self._distinct:
            function_sql = f"{self.name}(DISTINCT {arg_sql})"
        else:
            function_sql = f"{self.name}({arg_sql})"

        if self.alias:
            return f"{function_sql} AS {self.alias}"

        return function_sql


class Sum(AggregateFunction):
    """SUM function."""

    def __init__(self, field: Any, alias: Optional[str] = None):
        """Initialize SUM function."""
        super().__init__("SUM", field, alias)


class Count(AggregateFunction):
    """COUNT function."""

    def __init__(self, field: Any = "*", alias: Optional[str] = None):
        """Initialize COUNT function."""
        super().__init__("COUNT", field, alias)


class Avg(AggregateFunction):
    """AVG function."""

    def __init__(self, field: Any, alias: Optional[str] = None):
        """Initialize AVG function."""
        super().__init__("AVG", field, alias)


class Min(AggregateFunction):
    """MIN function."""

    def __init__(self, field: Any, alias: Optional[str] = None):
        """Initialize MIN function."""
        super().__init__("MIN", field, alias)


class Max(AggregateFunction):
    """MAX function."""

    def __init__(self, field: Any, alias: Optional[str] = None):
        """Initialize MAX function."""
        super().__init__("MAX", field, alias)