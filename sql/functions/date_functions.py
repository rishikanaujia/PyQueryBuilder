# pyquerybuilder/sql/functions/date_functions.py
"""Date and time SQL functions for PyQueryBuilder."""
from typing import Any, Optional

from .base_function import Function


class DatePart(Function):
    """Extract part of a date (year, month, day, etc.)."""

    def __init__(self, part: str, date: Any, alias: Optional[str] = None):
        """Initialize DATE_PART function.

        Args:
            part: Date part to extract (YEAR, MONTH, DAY, etc.)
            date: Date field or expression
            alias: Optional alias for the result
        """
        super().__init__("DATE_PART", part, date, alias=alias)

    def get_sql(self) -> str:
        """Generate SQL for DATE_PART function.

        Returns:
            SQL string representation
        """
        part = self.args[0]
        date = self.args[1]

        date_sql = date.get_sql() if hasattr(date, "get_sql") else str(date)

        function_sql = f"DATE_PART('{part}', {date_sql})"

        if self.alias:
            return f"{function_sql} AS {self.alias}"

        return function_sql


class DateTrunc(Function):
    """Truncate date to specified precision."""

    def __init__(self, precision: str, date: Any, alias: Optional[str] = None):
        """Initialize DATE_TRUNC function.

        Args:
            precision: Precision to truncate to (YEAR, MONTH, DAY, etc.)
            date: Date field or expression
            alias: Optional alias for the result
        """
        super().__init__("DATE_TRUNC", precision, date, alias=alias)

    def get_sql(self) -> str:
        """Generate SQL for DATE_TRUNC function.

        Returns:
            SQL string representation
        """
        precision = self.args[0]
        date = self.args[1]

        date_sql = date.get_sql() if hasattr(date, "get_sql") else str(date)

        function_sql = f"DATE_TRUNC('{precision}', {date_sql})"

        if self.alias:
            return f"{function_sql} AS {self.alias}"

        return function_sql


class CurrentDate(Function):
    """Get current date."""

    def __init__(self, alias: Optional[str] = None):
        """Initialize CURRENT_DATE function."""
        super().__init__("CURRENT_DATE", alias=alias)

    def get_sql(self) -> str:
        """Generate SQL for CURRENT_DATE function.

        Returns:
            SQL string representation
        """
        function_sql = "CURRENT_DATE()"

        if self.alias:
            return f"{function_sql} AS {self.alias}"

        return function_sql