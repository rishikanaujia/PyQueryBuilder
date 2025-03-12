# pyquerybuilder/query/analyzer.py
"""Analyzer for validating and preparing queries."""
from typing import Dict, List, Any, Optional


class QueryAnalyzer:
    """Analyzes query components and resolves dependencies."""

    def __init__(self, schema_registry):
        """Initialize with schema registry."""
        self.schema_registry = schema_registry

    def analyze(self, select_fields, from_table, joins=None,
                where_conditions=None, group_by=None,
                order_by=None, limit=None, offset=None):
        """Analyze and validate query components.

        Args:
            select_fields: List of fields to select
            from_table: Main table in FROM clause
            joins: List of join specifications
            where_conditions: List of WHERE conditions
            group_by: List of GROUP BY fields
            order_by: List of ORDER BY specifications
            limit: LIMIT value
            offset: OFFSET value

        Returns:
            Dictionary of analyzed query components
        """
        # Process FROM table and extract alias if present
        from_info = self._process_from_table(from_table)

        # Resolve and validate joins
        resolved_joins = self._resolve_joins(
            from_info, joins or []
        )