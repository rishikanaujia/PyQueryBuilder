# pyquerybuilder/discovery/snowflake/schema_reader.py
"""Functions for reading schema information from Snowflake."""
from typing import Dict, List, Any


def discover_tables(connector, include_views=True,
                    include_tables=None, exclude_tables=None):
    """Discover tables and views from Snowflake.

    Args:
        connector: Snowflake connector instance
        include_views: Whether to include views
        include_tables: Optional list of tables to include
        exclude_tables: Optional list of tables to exclude

    Returns:
        Dictionary of tables with their metadata
    """
    conn = connector.connect()
    cursor = conn.cursor()

    # Build table type filter
    table_type_filter = "TABLE"
    if include_views:
        table_type_filter = "('TABLE', 'VIEW')"

    # Query for tables
    query = f"""
    SELECT 
        TABLE_NAME, 
        TABLE_TYPE,
        TABLE_SCHEMA
    FROM 
        INFORMATION_SCHEMA.TABLES
    WHERE 
        TABLE_SCHEMA = '{connector.schema}'
        AND TABLE_TYPE IN {table_type_filter}
    """