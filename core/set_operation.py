# pyquerybuilder/core/set_operation.py
"""Support for set operations (UNION, INTERSECT, MINUS) in PyQueryBuilder."""
from typing import List, Optional, Tuple, Dict, Any
from enum import Enum


class SetOperationType(Enum):
    """Types of SQL set operations."""
    UNION = "UNION"
    UNION_ALL = "UNION ALL"
    INTERSECT = "INTERSECT"
    EXCEPT = "EXCEPT"
    MINUS = "MINUS"  # Oracle's equivalent of EXCEPT


class SetOperation:
    """Represents a set operation between two queries."""

    def __init__(self, left_query, operation_type, right_query, alias=None):
        """Initialize set operation.

        Args:
            left_query: Left side query
            operation_type: Type of set operation
            right_query: Right side query
            alias: Optional alias for the result
        """
        self.left_query = left_query
        self.operation_type = operation_type
        self.right_query = right_query
        self.alias = alias
        self._order_by = []
        self._limit = None
        self._offset = None

    def as_(self, alias):
        """Set alias for this set operation result.

        Args:
            alias: Alias name

        Returns:
            Self for method chaining
        """
        self.alias = alias
        return self

    def order_by(self, field, direction="ASC"):
        """Add ORDER BY clause to the set operation result.

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

    def limit(self, limit):
        """Add LIMIT clause to the set operation result.

        Args:
            limit: Maximum number of rows

        Returns:
            Self for method chaining
        """
        self._limit = limit
        return self

    def offset(self, offset):
        """Add OFFSET clause to the set operation result.

        Args:
            offset: Number of rows to skip

        Returns:
            Self for method chaining
        """
        self._offset = offset
        return self

    def get_sql(self):
        """Generate SQL for this set operation.

        Returns:
            SQL string representation
        """
        # Get SQL for left and right queries
        left_sql, left_params = self.left_query.build()
        right_sql, right_params = self.right_query.build()

        # Combine with set operation
        operation_sql = f"{left_sql} {self.operation_type.value} {right_sql}"

        # Add ORDER BY if specified
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

            operation_sql += f" ORDER BY {', '.join(order_terms)}"

        # Add LIMIT if specified
        if self._limit is not None:
            operation_sql += f" LIMIT {self._limit}"

            # Add OFFSET if specified
            if self._offset is not None:
                operation_sql += f" OFFSET {self._offset}"

        # Add parentheses and alias if needed
        if self.alias:
            return f"({operation_sql}) AS {self.alias}"

        return operation_sql

    def build(self):
        """Build SQL and parameters for this set operation.

        Returns:
            Tuple of (SQL string, parameters dict)
        """
        left_sql, left_params = self.left_query.build()
        right_sql, right_params = self.right_query.build()

        # Combine parameters from both queries
        # We need to ensure parameter names don't conflict
        params = {}
        for name, value in left_params.items():
            params[f"left_{name}"] = value
            left_sql = left_sql.replace(f":{name}", f":left_{name}")

        for name, value in right_params.items():
            params[f"right_{name}"] = value
            right_sql = right_sql.replace(f":{name}", f":right_{name}")

        # Build the combined SQL
        operation_sql = f"{left_sql} {self.operation_type.value} {right_sql}"

        # Add ORDER BY if specified
        if self._order_by:
            from ..sql.generators.order_generator import generate_order_by
            order_clause = generate_order_by(self._order_by)
            operation_sql += f" {order_clause}"

        # Add LIMIT and OFFSET if specified
        if self._limit is not None:
            operation_sql += f" LIMIT {self._limit}"

            if self._offset is not None:
                operation_sql += f" OFFSET {self._offset}"

        return operation_sql, params