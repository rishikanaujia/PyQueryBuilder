"""Core query builder interface for PyQueryBuilder."""
from typing import Any, Dict, List, Optional, Tuple, Union

from discovery.snowflake.metadata_inspector import MetadataInspector
from ..sql.functions import fn


class QueryBuilder:
    """Main query builder with fluent interface for SQL queries."""

    # Add the functions namespace as a class attribute for easy access
    fn = fn

    @classmethod
    def from_snowflake(cls, account, user, password,
                       warehouse, database, schema=None, **options):
        """Initialize with auto-discovered schema from Snowflake."""
        from ..discovery.snowflake.connector import SnowflakeConnector
        #from ..discovery.inspector import MetadataInspector
        from ..schema.registry import SchemaRegistry

        connector = SnowflakeConnector(
            account, user, password, warehouse, database, schema
        )

        inspector = MetadataInspector(connector)
        schema_metadata = inspector.discover_schema(**options)

        registry = SchemaRegistry()
        registry.register_schema(schema_metadata)

        return cls(schema_registry=registry, connector=connector)

    def select(self, *fields) -> 'QueryBuilder':
        """Add fields to the SELECT clause.

        Args:
            *fields: Field names or function objects to select

        Returns:
            Self for method chaining
        """
        for field in fields:
            # Handle function objects
            if hasattr(field, 'get_sql'):
                self._select_fields.append(field)
            else:
                # Handle string fields
                self._select_fields.append(field)
        return self

    def group_by(self, *fields) -> 'QueryBuilder':
        """Add fields to the GROUP BY clause.

        Args:
            *fields: Field names or function objects to group by

        Returns:
            Self for method chaining
        """
        for field in fields:
            # Handle function objects
            if hasattr(field, 'get_sql'):
                self._group_by.append(field)
            else:
                # Handle string fields
                self._group_by.append(field)
        return self

    def order_by(self, field, direction: str = "asc") -> 'QueryBuilder':
        """Add a field to the ORDER BY clause.

        Args:
            field: Field or function to order by
            direction: Sort direction (asc or desc)

        Returns:
            Self for method chaining
        """
        self._order_by.append({
            "field": field,
            "direction": direction
        })
        return self

    # pyquerybuilder/core/builder.py
    # Add these methods to the QueryBuilder class

    def as_subquery(self, alias: Optional[str] = None) -> "Subquery":
        """Convert this query to a subquery that can be used in another query.

        Args:
            alias: Optional alias for the subquery

        Returns:
            Subquery object
        """
        from .subquery import Subquery
        return Subquery(self, alias)

    def from_subquery(self, subquery, alias: Optional[str] = None) -> "QueryBuilder":
        """Set a subquery as the FROM clause.

        Args:
            subquery: QueryBuilder instance or Subquery
            alias: Optional alias for the subquery

        Returns:
            Self for method chaining
        """
        # If it's a QueryBuilder, convert to Subquery
        from .subquery import Subquery
        if isinstance(subquery, QueryBuilder) and not isinstance(subquery, Subquery):
            subquery = subquery.as_subquery(alias)
        elif alias and hasattr(subquery, 'as_'):
            subquery = subquery.as_(alias)

        # Store the subquery as the FROM source
        self._from_subquery = subquery
        self._from_table = None  # Clear any existing FROM table

        return self

    # pyquerybuilder/core/builder.py
    # Update the build method

    def build(self) -> Tuple[str, Dict[str, Any]]:
        """Build the SQL query and parameter dictionary."""
        from ..query.analyzer import QueryAnalyzer
        from ..sql.generator import SQLGenerator

        # Analyze the query to resolve joins and validate fields
        analyzer = QueryAnalyzer(self._schema_registry)
        analyzed_query = analyzer.analyze(
            select_fields=self._select_fields,
            from_table=self._from_table,
            from_subquery=self._from_subquery,
            joins=self._joins,
            where_conditions=self._where_conditions,
            group_by=self._group_by,
            order_by=self._order_by,
            limit=self._limit,
            offset=self._offset
        )

        # Generate SQL from the analyzed query
        generator = SQLGenerator(dialect="snowflake")
        sql, params = generator.generate(analyzed_query)

        return sql, params




