"""Core query builder interface for PyQueryBuilder."""
from typing import Any, Dict, List, Optional, Tuple, Union


class QueryBuilder:
    """Main query builder with fluent interface for SQL queries."""

    @classmethod
    def from_snowflake(cls, account, user, password,
                       warehouse, database, schema=None, **options):
        """Initialize with auto-discovered schema from Snowflake."""
        from ..discovery.snowflake.connector import SnowflakeConnector
        from ..discovery.inspector import MetadataInspector
        from ..schema.registry import SchemaRegistry

        connector = SnowflakeConnector(
            account, user, password, warehouse, database, schema
        )

        inspector = MetadataInspector(connector)
        schema_metadata = inspector.discover_schema(**options)

        registry = SchemaRegistry()
        registry.register_schema(schema_metadata)

        return cls(schema_registry=registry, connector=connector)