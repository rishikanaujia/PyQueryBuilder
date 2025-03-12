# pyquerybuilder/discovery/snowflake/connector.py
"""Connector for Snowflake database interaction."""
import snowflake.connector
from typing import Dict, List, Any, Optional


class SnowflakeConnector:
    """Manages connection to Snowflake and executes queries."""

    def __init__(self, account, user, password, warehouse,
                 database, schema=None):
        """Initialize Snowflake connection parameters."""
        self.account = account
        self.user = user
        self.password = password
        self.warehouse = warehouse
        self.database = database
        self.schema = schema or "PUBLIC"
        self._connection = None

    def connect(self):
        """Establish connection to Snowflake."""
        if not self._connection:
            self._connection = snowflake.connector.connect(
                user=self.user,
                password=self.password,
                account=self.account,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema
            )
        return self._connection