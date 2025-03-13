# pyquerybuilder/query/where_group.py
"""Support for complex WHERE clause grouping in PyQueryBuilder."""
from typing import Any, List, Optional


class WhereGroup:
    """Group of WHERE conditions that can be combined with AND/OR logic."""

    def __init__(self):
        """Initialize empty WHERE group."""
        self.conditions = []
        self.conjunction = "AND"  # Default conjunction

    def where(self, field, operator, value=None):
        """Add a condition to the group with AND logic.

        Args:
            field: Field name or function
            operator: Comparison operator
            value: Value to compare against (optional for IS NULL, etc.)

        Returns:
            Self for method chaining
        """
        if value is None and operator.upper() not in ("IS NULL", "IS NOT NULL"):
            # Shift parameters for convenience: field, value with implied "="
            value = operator
            operator = "="

        self.conditions.append({
            "field": field,
            "operator": operator,
            "value": value,
            "type": "condition"
        })
        return self

    def or_where(self, field, operator=None, value=None):
        """Add a condition to the group with OR logic.

        Args:
            field: Field name, function, or another WhereGroup
            operator: Comparison operator (optional if field is WhereGroup)
            value: Value to compare against (optional)

        Returns:
            Self for method chaining
        """
        # Handle nested WhereGroup
        if isinstance(field, WhereGroup):
            self.conditions.append({
                "group": field,
                "type": "or_group"
            })
            return self

        # Handle normal condition
        if value is None and operator is not None:
            # Shift parameters for convenience
            value = operator
            operator = "="

        self.conditions.append({
            "field": field,
            "operator": operator,
            "value": value,
            "type": "or_condition"
        })
        return self

    def and_where_group(self, group):
        """Add a nested where group with AND logic.

        Args:
            group: WhereGroup instance

        Returns:
            Self for method chaining
        """
        self.conditions.append({
            "group": group,
            "type": "and_group"
        })
        return self

    def or_where_group(self, group):
        """Add a nested where group with OR logic.

        Args:
            group: WhereGroup instance

        Returns:
            Self for method chaining
        """
        self.conditions.append({
            "group": group,
            "type": "or_group"
        })
        return self

    def where_in(self, field, values):
        """Add an IN condition to the group.

        Args:
            field: Field name or function
            values: List of values or subquery

        Returns:
            Self for method chaining
        """
        self.conditions.append({
            "field": field,
            "operator": "IN",
            "value": values,
            "type": "condition"
        })
        return self

    def where_not_in(self, field, values):
        """Add a NOT IN condition to the group.

        Args:
            field: Field name or function
            values: List of values or subquery

        Returns:
            Self for method chaining
        """
        self.conditions.append({
            "field": field,
            "operator": "NOT IN",
            "value": values,
            "type": "condition"
        })
        return self

    def where_between(self, field, start, end):
        """Add a BETWEEN condition to the group.

        Args:
            field: Field name or function
            start: Lower bound
            end: Upper bound

        Returns:
            Self for method chaining
        """
        self.conditions.append({
            "field": field,
            "operator": "BETWEEN",
            "value": (start, end),
            "type": "condition"
        })
        return self

    def where_not_between(self, field, start, end):
        """Add a NOT BETWEEN condition to the group.

        Args:
            field: Field name or function
            start: Lower bound
            end: Upper bound

        Returns:
            Self for method chaining
        """
        self.conditions.append({
            "field": field,
            "operator": "NOT BETWEEN",
            "value": (start, end),
            "type": "condition"
        })
        return self