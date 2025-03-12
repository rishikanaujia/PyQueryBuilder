# pyquerybuilder/query/analyzer.py
"""Analyzer for validating and preparing queries."""
from typing import Dict, List, Any, Optional

from .analyzers.field_analyzer import analyze_fields
from .analyzers.join_analyzer import analyze_joins
from .analyzers.join_resolver import resolve_join
from .analyzers.join_path_finder import find_join_path


class QueryAnalyzer:
    """Analyzes query components and resolves dependencies."""

    def __init__(self, schema_registry):
        """Initialize with schema registry."""
        self.schema_registry = schema_registry

    def analyze(self, select_fields, from_table, joins=None,
                where_conditions=None, group_by=None,
                order_by=None, limit=None, offset=None):
        """Analyze and validate query components."""
        # Process field references and determine requirements
        field_analysis = analyze_fields(
            select_fields, self.schema_registry
        )

        # Process FROM table and extract alias if present
        from_info = self._process_from_table(from_table)

        # Resolve and validate joins
        join_analysis = analyze_joins(
            from_info, joins or [], field_analysis["required_tables"],
            self.schema_registry
        )

        # Return analyzed query components
        return {
            "select_fields": field_analysis["field_info"],
            "from_table": from_info,
            "joins": join_analysis["resolved_joins"],
            "where_conditions": where_conditions or [],
            "group_by": group_by or [],
            "order_by": order_by or [],
            "limit": limit,
            "offset": offset
        }