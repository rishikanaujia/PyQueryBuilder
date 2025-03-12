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

    # Add filter for specific tables if provided
    if include_tables:
        tables_list = "', '".join(include_tables)
        query += f" AND TABLE_NAME IN ('{tables_list}')"

    # Add exclusion filter if provided
    if exclude_tables:
        tables_list = "', '".join(exclude_tables)
        query += f" AND TABLE_NAME NOT IN ('{tables_list}')"

    cursor.execute(query)

    tables = {}
    for row in cursor.fetchall():
        table_name = row[0]
        table_type = row[1]
        schema = row[2]

        # Generate a simple alias (first letter of table name)
        alias = table_name[0].lower()

        tables[table_name] = {
            "name": table_name,
            "type": table_type,
            "schema": schema,
            "alias": alias
        }

    cursor.close()
    return tables


def discover_columns(connector, tables):
    """Discover columns for the specified tables.

    Args:
        connector: Snowflake connector instance
        tables: Dictionary of tables from discover_tables

    Returns:
        Dictionary mapping table names to their columns
    """
    conn = connector.connect()
    cursor = conn.cursor()

    # Build list of table names
    table_names = list(tables.keys())
    if not table_names:
        return {}

    # Query for columns
    tables_list = "', '".join(table_names)
    query = f"""
    SELECT 
        TABLE_NAME,
        COLUMN_NAME,
        DATA_TYPE,
        IS_NULLABLE,
        CHARACTER_MAXIMUM_LENGTH,
        NUMERIC_PRECISION,
        NUMERIC_SCALE
    FROM 
        INFORMATION_SCHEMA.COLUMNS
    WHERE 
        TABLE_SCHEMA = '{connector.schema}'
        AND TABLE_NAME IN ('{tables_list}')
    ORDER BY 
        TABLE_NAME, ORDINAL_POSITION
    """

    cursor.execute(query)

    columns = {}
    for row in cursor.fetchall():
        table_name = row[0]
        column_name = row[1]
        data_type = row[2]
        is_nullable = row[3]
        char_max_length = row[4]
        numeric_precision = row[5]
        numeric_scale = row[6]

        if table_name not in columns:
            columns[table_name] = {}

        columns[table_name][column_name] = {
            "name": column_name,
            "type": data_type,
            "nullable": is_nullable == "YES",
            "max_length": char_max_length,
            "precision": numeric_precision,
            "scale": numeric_scale
        }

    cursor.close()
    return columns


def discover_relationships(connector, tables, columns):
    """Discover relationships between tables using foreign keys.

    Args:
        connector: Snowflake connector instance
        tables: Dictionary of tables from discover_tables
        columns: Dictionary of columns from discover_columns

    Returns:
        Dictionary of relationships
    """
    conn = connector.connect()
    cursor = conn.cursor()

    # Query for foreign key relationships
    query = f"""
    SELECT 
        rc.CONSTRAINT_NAME,
        rc.TABLE_NAME as source_table,
        kcu.COLUMN_NAME as source_column,
        kcu.REFERENCED_TABLE_NAME as target_table,
        kcu.REFERENCED_COLUMN_NAME as target_column
    FROM 
        INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
    JOIN 
        INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
        ON rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
        AND rc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
    WHERE 
        rc.CONSTRAINT_SCHEMA = '{connector.schema}'
        AND rc.TABLE_NAME IN ('{("', '".join(tables.keys()))}')
    """

    relationships = {}
    try:
        cursor.execute(query)

        for row in cursor.fetchall():
            constraint_name = row[0]
            source_table = row[1]
            source_column = row[2]
            target_table = row[3]
            target_column = row[4]

            relationships[constraint_name] = {
                "name": constraint_name,
                "source_table": source_table,
                "source_column": source_column,
                "target_table": target_table,
                "target_column": target_column,
                "type": "FOREIGN_KEY"
            }
    except Exception as e:
        # Some Snowflake editions might not support referential constraints
        # In that case, we'll infer relationships from naming conventions
        pass

    # If no relationships found through FK constraints, try to infer them
    if not relationships:
        relationships = _infer_relationships(tables, columns)

    cursor.close()
    return relationships


def _infer_relationships(tables, columns):
    """Infer relationships between tables based on naming conventions.

    Args:
        tables: Dictionary of tables
        columns: Dictionary of columns

    Returns:
        Dictionary of inferred relationships
    """
    relationships = {}
    relationship_id = 0

    for source_table, source_cols in columns.items():
        for source_col, source_info in source_cols.items():
            # Look for columns with _id suffix or id in the name
            if (source_col.lower().endswith('_id') or
                    source_col.lower() == 'id' or
                    '_id_' in source_col.lower()):

                # Potential target table name (remove _id suffix)
                if source_col.lower().endswith('_id'):
                    target_table_guess = source_col[:-3]
                else:
                    # Try to extract table name from column name
                    parts = source_col.lower().split('_')
                    target_table_guess = next((p for p in parts
                                               if p != 'id'), None)

                if not target_table_guess:
                    continue

                # See if any table name matches our guess
                target_table = None
                for table_name in tables:
                    if (table_name.lower() == target_table_guess or
                            table_name.lower() == target_table_guess + 's' or
                            table_name.lower() == target_table_guess + 'es'):
                        target_table = table_name
                        break

                if target_table and target_table in columns:
                    # Look for id column in target table
                    target_column = None
                    for col in columns[target_table]:
                        if col.lower() == 'id':
                            target_column = col
                            break

                    if target_column:
                        relationship_id += 1
                        rel_name = f"inferred_rel_{relationship_id}"
                        relationships[rel_name] = {
                            "name": rel_name,
                            "source_table": source_table,
                            "source_column": source_col,
                            "target_table": target_table,
                            "target_column": target_column,
                            "type": "INFERRED"
                        }

    return relationships
