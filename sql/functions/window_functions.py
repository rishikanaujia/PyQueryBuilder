# pyquerybuilder/sql/functions/window_functions.py
"""Window functions for PyQueryBuilder."""
from typing import Any, List, Optional

from .base_function import Function


class WindowFunction(Function):
    """Base class for window functions with OVER clause."""

    def __init__(self, name: str, *args, alias: Optional[str] = None):
        """Initialize window function.

        Args:
            name: Function name
            *args: Function arguments
            alias: Optional alias for the result
        """
        super().__init__(name, *args, alias=alias)
        self._partition_by = []
        self._order_by = []
        self._frame_type = None
        self._frame_start = None
        self._frame_end = None

    def over(self) -> "WindowFunction":
        """Start building an OVER clause.

        Returns:
            Self for method chaining
        """
        return self

    def partition_by(self, *fields) -> "WindowFunction":
        """Add PARTITION BY clause to the window function.

        Args:
            *fields: Fields to partition by

        Returns:
            Self for method chaining
        """
        for field in fields:
            self._partition_by.append(field)
        return self

    def order_by(self, field, direction: str = "ASC") -> "WindowFunction":
        """Add ORDER BY clause to the window function.

        Args:
            field: Field to order by
            direction: Sort direction (ASC or DESC)

        Returns:
            Self for method chaining
        """
        self._order_by.append({
            "field": field,
            "direction": direction.upper()
        })
        return self

    def rows(self, start_expr=None, end_expr=None) -> "WindowFunction":
        """Add ROWS frame to the window function.

        Args:
            start_expr: Frame start expression
            end_expr: Frame end expression

        Returns:
            Self for method chaining
        """
        self._frame_type = "ROWS"
        self._frame_start = start_expr
        self._frame_end = end_expr
        return self

    def range(self, start_expr=None, end_expr=None) -> "WindowFunction":
        """Add RANGE frame to the window function.

        Args:
            start_expr: Frame start expression
            end_expr: Frame end expression

        Returns:
            Self for method chaining
        """
        self._frame_type = "RANGE"
        self._frame_start = start_expr
        self._frame_end = end_expr
        return self

    def get_sql(self) -> str:
        """Generate SQL for this window function.

        Returns:
            SQL string representation
        """
        args_sql = ", ".join(
            arg.get_sql() if hasattr(arg, "get_sql") else str(arg)
            for arg in self.args
        )

        function_sql = f"{self.name}({args_sql})"

        # Add OVER clause if partitioning or ordering is defined
        if self._partition_by or self._order_by:
            over_parts = []

            # Add PARTITION BY
            if self._partition_by:
                partition_fields = []
                for field in self._partition_by:
                    if hasattr(field, "get_sql"):
                        partition_fields.append(field.get_sql())
                    else:
                        partition_fields.append(str(field))

                over_parts.append(f"PARTITION BY {', '.join(partition_fields)}")

            # Add ORDER BY
            if self._order_by:
                order_terms = []
                for order_spec in self._order_by:
                    field = order_spec["field"]
                    direction = order_spec["direction"]

                    if hasattr(field, "get_sql"):
                        field_sql = field.get_sql()
                    else:
                        field_sql = str(field)

                    order_terms.append(f"{field_sql} {direction}")

                over_parts.append(f"ORDER BY {', '.join(order_terms)}")

            # Add window frame if specified
            if self._frame_type:
                frame_sql = self._frame_type

                if self._frame_start is not None and self._frame_end is not None:
                    # BETWEEN frame
                    start_sql = self._get_frame_expr_sql(self._frame_start)
                    end_sql = self._get_frame_expr_sql(self._frame_end)
                    frame_sql += f" BETWEEN {start_sql} AND {end_sql}"
                elif self._frame_start is not None:
                    # Single bound frame
                    start_sql = self._get_frame_expr_sql(self._frame_start)
                    frame_sql += f" {start_sql}"

                over_parts.append(frame_sql)

            function_sql += f" OVER ({' '.join(over_parts)})"

        if self.alias:
            return f"{function_sql} AS {self.alias}"

        return function_sql

    def _get_frame_expr_sql(self, expr) -> str:
        """Convert frame expression to SQL.

        Args:
            expr: Frame expression (e.g., "UNBOUNDED PRECEDING")

        Returns:
            SQL string for frame expression
        """
        if isinstance(expr, str) and expr.upper() in (
                "CURRENT ROW", "UNBOUNDED PRECEDING", "UNBOUNDED FOLLOWING"
        ):
            return expr

        if isinstance(expr, int):
            if expr < 0:
                return f"{abs(expr)} PRECEDING"
            elif expr > 0:
                return f"{expr} FOLLOWING"
            else:
                return "CURRENT ROW"

        # If it's already a valid SQL fragment
        return str(expr)


# pyquerybuilder/sql/functions/window_functions.py
# Add these classes to the same file

class Rank(WindowFunction):
    """RANK() window function."""

    def __init__(self, alias: Optional[str] = None):
        """Initialize RANK function."""
        super().__init__("RANK", alias=alias)


class DenseRank(WindowFunction):
    """DENSE_RANK() window function."""

    def __init__(self, alias: Optional[str] = None):
        """Initialize DENSE_RANK function."""
        super().__init__("DENSE_RANK", alias=alias)


class RowNumber(WindowFunction):
    """ROW_NUMBER() window function."""

    def __init__(self, alias: Optional[str] = None):
        """Initialize ROW_NUMBER function."""
        super().__init__("ROW_NUMBER", alias=alias)


class Lead(WindowFunction):
    """LEAD() window function."""

    def __init__(self, expr, offset: int = 1, default=None, alias: Optional[str] = None):
        """Initialize LEAD function.

        Args:
            expr: Expression to evaluate
            offset: Number of rows to lead
            default: Default value if lead goes beyond partition
            alias: Optional alias for the result
        """
        if default is not None:
            super().__init__("LEAD", expr, offset, default, alias=alias)
        else:
            super().__init__("LEAD", expr, offset, alias=alias)


class Lag(WindowFunction):
    """LAG() window function."""

    def __init__(self, expr, offset: int = 1, default=None, alias: Optional[str] = None):
        """Initialize LAG function.

        Args:
            expr: Expression to evaluate
            offset: Number of rows to lag
            default: Default value if lag goes beyond partition
            alias: Optional alias for the result
        """
        if default is not None:
            super().__init__("LAG", expr, offset, default, alias=alias)
        else:
            super().__init__("LAG", expr, offset, alias=alias)


class FirstValue(WindowFunction):
    """FIRST_VALUE() window function."""

    def __init__(self, expr, alias: Optional[str] = None):
        """Initialize FIRST_VALUE function."""
        super().__init__("FIRST_VALUE", expr, alias=alias)


class LastValue(WindowFunction):
    """LAST_VALUE() window function."""

    def __init__(self, expr, alias: Optional[str] = None):
        """Initialize LAST_VALUE function."""
        super().__init__("LAST_VALUE", expr, alias=alias)


class NTile(WindowFunction):
    """NTILE() window function."""

    def __init__(self, buckets: int, alias: Optional[str] = None):
        """Initialize NTILE function.

        Args:
            buckets: Number of buckets to divide the rows into
            alias: Optional alias for the result
        """
        super().__init__("NTILE", buckets, alias=alias)