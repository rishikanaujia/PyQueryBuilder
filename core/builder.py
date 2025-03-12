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




