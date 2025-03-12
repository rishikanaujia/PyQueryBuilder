# pyquerybuilder/discovery/metadata_inspector.py
"""Inspector for discovering database metadata."""
from typing import Dict, List, Any


class MetadataInspector:
    """Discovers and extracts database schema metadata."""

    def __init__(self, connector):
        """Initialize with database connector."""
        self.connector = connector

    def discover_schema(self, include_tables=None,
                        exclude_tables=None, include_views=True):
        """Discover database schema metadata.

        Args:
            include_tables: Optional list of tables to include
            exclude_tables: Optional list of tables to exclude
            include_views: Whether to include views

        Returns:
            Dictionary containing schema metadata
        """
        # Import appropriate schema reader based on connector type
        if hasattr(self.connector, 'database'):
            # Snowflake connector
            from .snowflake.schema_reader import (
                discover_tables,
                discover_columns,
                discover_relationships
            )
        else:
            # Default generic reader (can be expanded later)
            from .generic.schema_reader import (
                discover_tables,
                discover_columns,
                discover_relationships
            )

        # Discover table definitions
        tables = discover_tables(
            self.connector,
            include_views=include_views,
            include_tables=include_tables,
            exclude_tables=exclude_tables
        )

        # Discover column definitions
        columns = discover_columns(self.connector, tables)

        # Discover relationships between tables
        relationships = discover_relationships(
            self.connector, tables, columns
        )

        # Assemble complete schema information
        return {
            "tables": tables,
            "columns": columns,
            "relationships": relationships
        }