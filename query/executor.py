# pyquerybuilder/core/executor.py
"""Executor for running SQL queries."""
from typing import Dict, List, Any


class QueryExecutor:
    """Executes SQL queries and processes results."""

    def __init__(self, connector):
        """Initialize with database connector."""
        self.connector = connector

    def execute(self, sql, params=None):
        """Execute a SQL query with parameters.

        Args:
            sql: SQL query string
            params: Optional parameters dictionary

        Returns:
            List of dictionaries with query results
        """
        # Connect to database
        connection = self.connector.connect()
        cursor = connection.cursor()

        try:
            # Execute query
            cursor.execute(sql, params or {})

            # Fetch column names
            column_names = [desc[0] for desc in cursor.description]

            # Fetch and process results
            results = []
            for row in cursor.fetchall():
                result = dict(zip(column_names, row))
                results.append(result)

            return results
        finally:
            cursor.close()