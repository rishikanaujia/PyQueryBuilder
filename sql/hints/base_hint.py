# pyquerybuilder/sql/hints/base_hint.py
"""Base classes for database-specific query hints."""
from typing import Any, Optional, Dict


class QueryHint:
    """Base class for database query hints."""

    def __init__(self, hint_type: str, value: Any = None):
        """Initialize a query hint.

        Args:
            hint_type: Type of hint
            value: Optional value for the hint
        """
        self.hint_type = hint_type
        self.value = value

    def get_sql(self) -> str:
        """Generate SQL for this hint.

        Returns:
            SQL string for the hint
        """
        raise NotImplementedError("Subclasses must implement get_sql")


class Comment(QueryHint):
    """SQL comment hint."""

    def __init__(self, text: str):
        """Initialize a comment hint.

        Args:
            text: Comment text
        """
        super().__init__("COMMENT", text)

    def get_sql(self) -> str:
        """Generate SQL for a comment.

        Returns:
            SQL comment string
        """
        return f"/* {self.value} */"