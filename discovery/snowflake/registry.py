# pyquerybuilder/schema/registry.py
"""Registry for managing discovered schema information."""
from typing import Dict, List, Any, Optional


class SchemaRegistry:
    """Central registry for schema metadata."""

    def __init__(self):
        """Initialize empty registry."""
        self.tables = {}
        self.columns = {}
        self.relationships = {}
        self.join_paths = {}

    def register_schema(self, schema_metadata):
        """Register discovered schema metadata.

        Args:
            schema_metadata: Dictionary with tables, columns,
                            and relationships
        """
        self.tables = schema_metadata.get("tables", {})
        self.columns = schema_metadata.get("columns", {})
        self.relationships = schema_metadata.get("relationships", {})
        self._build_join_paths()

    def _build_join_paths(self):
        """Build join paths from relationships."""
        # Process foreign key relationships
        for rel_id, rel in self.relationships.items():
            self._add_join_path(
                rel["source_table"],
                rel["target_table"],
                rel["source_column"],
                rel["target_column"]
            )