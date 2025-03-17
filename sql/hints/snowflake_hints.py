# pyquerybuilder/sql/hints/snowflake_hints.py
"""Snowflake-specific query hints."""
from typing import Any, Dict, List, Optional, Union

from .base_hint import QueryHint


class SnowflakeQueryTag(QueryHint):
    """Snowflake QUERY_TAG hint."""

    def __init__(self, tag: str):
        """Initialize a QUERY_TAG hint.

        Args:
            tag: Query tag value
        """
        super().__init__("QUERY_TAG", tag)

    def get_sql(self) -> str:
        """Generate SQL for Snowflake QUERY_TAG.

        Returns:
            SQL string for the hint
        """
        return f"/*+ QUERY_TAG('{self.value}') */"


class SnowflakeWarehouse(QueryHint):
    """Snowflake USE_WAREHOUSE hint."""

    def __init__(self, warehouse: str):
        """Initialize a USE_WAREHOUSE hint.

        Args:
            warehouse: Warehouse name
        """
        super().__init__("USE_WAREHOUSE", warehouse)

    def get_sql(self) -> str:
        """Generate SQL for Snowflake USE_WAREHOUSE.

        Returns:
            SQL string for the hint
        """
        return f"/*+ USE_WAREHOUSE({self.value}) */"


class SnowflakeAutoCluster(QueryHint):
    """Snowflake AUTO_CLUSTER hint."""

    def __init__(self, enabled: bool = True):
        """Initialize an AUTO_CLUSTER hint.

        Args:
            enabled: Whether auto-clustering is enabled
        """
        super().__init__("AUTO_CLUSTER", "ON" if enabled else "OFF")

    def get_sql(self) -> str:
        """Generate SQL for Snowflake AUTO_CLUSTER.

        Returns:
            SQL string for the hint
        """
        return f"/*+ AUTO_CLUSTER({self.value}) */"


class SnowflakeStreamline(QueryHint):
    """Snowflake STREAMLINE hint."""

    def __init__(self):
        """Initialize a STREAMLINE hint."""
        super().__init__("STREAMLINE")

    def get_sql(self) -> str:
        """Generate SQL for Snowflake STREAMLINE.

        Returns:
            SQL string for the hint
        """
        return "/*+ STREAMLINE */"