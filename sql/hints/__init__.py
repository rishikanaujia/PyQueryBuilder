# pyquerybuilder/sql/hints/__init__.py
"""Query hints for database-specific optimizations."""
from .base_hint import QueryHint, Comment
from .snowflake_hints import (
    SnowflakeQueryTag,
    SnowflakeWarehouse,
    SnowflakeAutoCluster,
    SnowflakeStreamline
)


class HintFactory:
    """Factory for creating database-specific hints."""

    @staticmethod
    def query_tag(tag: str) -> QueryHint:
        """Create a query tag hint.

        Args:
            tag: Query tag value

        Returns:
            Database-specific query tag hint
        """
        return SnowflakeQueryTag(tag)

    @staticmethod
    def use_warehouse(warehouse: str) -> QueryHint:
        """Create a warehouse hint.

        Args:
            warehouse: Warehouse name

        Returns:
            Database-specific warehouse hint
        """
        return SnowflakeWarehouse(warehouse)

    @staticmethod
    def auto_cluster(enabled: bool = True) -> QueryHint:
        """Create an auto-cluster hint.

        Args:
            enabled: Whether auto-clustering is enabled

        Returns:
            Database-specific auto-cluster hint
        """
        return SnowflakeAutoCluster(enabled)

    @staticmethod
    def streamline() -> QueryHint:
        """Create a streamline hint.

        Returns:
            Database-specific streamline hint
        """
        return SnowflakeStreamline()

    @staticmethod
    def comment(text: str) -> QueryHint:
        """Create a SQL comment.

        Args:
            text: Comment text

        Returns:
            SQL comment hint
        """
        return Comment(text)


# Export as 'hints' for convenience
hints = HintFactory