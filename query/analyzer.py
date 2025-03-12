# pyquerybuilder/query/analyzer.py
"""Analyzer for validating and preparing queries."""
from typing import Dict, List, Any, Optional

from .analyzers.field_analyzer import analyze_fields
from .analyzers.join_analyzer import analyze_joins


class QueryAnalyzer:
    """Analyzes query components and resolves dependencies."""

    def __init__(self, schema_registry):
        """Initialize with schema registry."""
        self.schema_registry = schema_registry

    def analyze(self, select_fields, from_table, joins=None,
                where_conditions=None, group_by=None,
                order_by=None, limit=None, offset=None):
        """Analyze and validate query components."""
        # Process FROM table and extract alias if present
        from_info = self._process_from_table(from_table)

        # Process field references and determine requirements
        field_analysis = analyze_fields(
            select_fields, self.schema_registry
        )

        # Resolve and validate joins
        join_analysis = analyze_joins(
            from_info, joins or [], field_analysis["required_tables"],
            self.schema_registry
        )

        # Process where conditions
        analyzed_where = self._analyze_where_conditions(
            where_conditions or []
        )

        # Return analyzed query components
        return {
            "select_fields": field_analysis["field_info"],
            "from_table": from_info,
            "joins": join_analysis["resolved_joins"],
            "where_conditions": analyzed_where,
            "group_by": group_by or [],
            "order_by": order_by or [],
            "limit": limit,
            "offset": offset
        }

    def _process_from_table(self, from_table):
        """Process the FROM table specification."""
        # Check if table has alias
        if " as " in from_table.lower():
            table, alias = from_table.lower().split(" as ", 1)
            return {"table": table.strip(), "alias": alias.strip()}
        elif " AS " in from_table:
            table, alias = from_table.split(" AS ", 1)
            return {"table": table.strip(), "alias": alias.strip()}
        else:
            # No alias specified
            return {"table": from_table.strip()}

    def _analyze_where_conditions(self, where_conditions):
        """Analyze and validate WHERE conditions."""
        # For now, just pass through the conditions
        # In a more complex implementation, we could validate
        # field references here as well
        return where_conditions