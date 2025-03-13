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

    def analyze(self, select_fields, from_table=None, from_subquery=None,
                joins=None, where_conditions=None, where_groups=None,
                group_by=None, order_by=None, limit=None, offset=None,
                with_ctes=None):
        """Analyze and validate query components."""
        # Either from_table or from_subquery must be provided
        if from_table:
            from_info = self._process_from_table(from_table)
            from_subquery_info = None
        elif from_subquery:
            from_info = None
            from_subquery_info = from_subquery
        else:
            raise ValueError("Either from_table or from_subquery must be provided")

        # Process field references and determine requirements
        field_analysis = analyze_fields(
            select_fields, self.schema_registry
        )

        # Resolve and validate joins
        if from_info:
            join_analysis = analyze_joins(
                from_info, joins or [], field_analysis["required_tables"],
                self.schema_registry
            )
        else:
            # With a subquery, we may not be able to auto-resolve joins
            join_analysis = {"resolved_joins": joins or []}

        # Process where conditions
        analyzed_where = self._analyze_where_conditions(
            where_conditions or []
        )

        # Process where groups - added this line
        analyzed_where_groups = self._analyze_where_groups(
            where_groups or []
        )

        # Return analyzed query components
        return {
            "select_fields": field_analysis["field_info"],
            "from_table": from_info,
            "from_subquery": from_subquery_info,
            "joins": join_analysis["resolved_joins"],
            "where_conditions": analyzed_where,
            "where_groups": analyzed_where_groups,
            "group_by": self._analyze_group_by(group_by or []),
            "order_by": self._analyze_order_by(order_by or []),
            "limit": limit,
            "offset": offset,
            "with_ctes": with_ctes or []  # Add this line
        }

    def _process_from_table(self, from_table):
        """Process the FROM table specification.

        Args:
            from_table: Table name (can include alias)

        Returns:
            Dictionary with table name and alias
        """
        # Check if table has alias
        if isinstance(from_table, str):
            if " as " in from_table.lower():
                table, alias = from_table.lower().split(" as ", 1)
                return {"table": table.strip(), "alias": alias.strip()}
            elif " AS " in from_table:
                table, alias = from_table.split(" AS ", 1)
                return {"table": table.strip(), "alias": alias.strip()}
            else:
                # No alias specified
                return {"table": from_table.strip()}
        else:
            # Already a dictionary or a subquery object
            return from_table

    def _analyze_where_conditions(self, where_conditions):
        """Analyze and validate WHERE conditions.

        Args:
            where_conditions: List of condition dictionaries

        Returns:
            Processed list of conditions
        """
        analyzed_conditions = []

        for condition in where_conditions:
            # Handle function objects in field or value
            field = condition.get("field")
            value = condition.get("value")

            # Clone the condition to avoid modifying the original
            analyzed_condition = condition.copy()

            # Handle field references, functions, etc.
            if hasattr(field, 'get_sql'):
                # Field is already a function or other SQL-generating object
                pass
            elif isinstance(field, str):
                # For string fields, could resolve table/field references
                # This is optional and can be expanded later
                pass

            analyzed_conditions.append(analyzed_condition)

        return analyzed_conditions

    def _analyze_group_by(self, group_by_fields):
        """Analyze and validate GROUP BY fields.

        Args:
            group_by_fields: List of fields to group by

        Returns:
            Processed list of GROUP BY fields
        """
        analyzed_fields = []

        for field in group_by_fields:
            # Handle function objects
            if hasattr(field, 'get_sql'):
                analyzed_fields.append(field)
            else:
                # For string fields, could resolve table/field references
                # This is optional and can be expanded later
                analyzed_fields.append(field)

        return analyzed_fields

    def _analyze_order_by(self, order_by_specs):
        """Analyze and validate ORDER BY specifications.

        Args:
            order_by_specs: List of ORDER BY specifications

        Returns:
            Processed list of ORDER BY specifications
        """
        analyzed_specs = []

        for spec in order_by_specs:
            # Handle function objects in field
            field = spec.get("field")
            direction = spec.get("direction", "asc")

            # Clone the spec to avoid modifying the original
            analyzed_spec = spec.copy()

            # Handle field references, functions, etc.
            if hasattr(field, 'get_sql'):
                # Field is already a function or other SQL-generating object
                pass
            elif isinstance(field, str):
                # For string fields, could resolve table/field references
                # This is optional and can be expanded later
                pass

            analyzed_specs.append(analyzed_spec)

        return analyzed_specs

    # Add this method to the QueryAnalyzer class
    def _analyze_where_groups(self, where_groups):
        """Analyze and validate WHERE groups.

        Args:
            where_groups: List of WhereGroup instances

        Returns:
            Processed list of WHERE groups
        """
        # For now, just pass through the groups
        # In a more complex implementation, we could validate
        # field references and extract table dependencies
        return where_groups