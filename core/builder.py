"""Core query builder interface for PyQueryBuilder."""
from typing import Any, Dict, List, Optional, Tuple, Union

from discovery.snowflake.metadata_inspector import MetadataInspector
from query.where_group import WhereGroup
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

        # Analyze the query
        analyzer = QueryAnalyzer(self._schema_registry)
        analyzed_query = analyzer.analyze(
            select_fields=self._select_fields,
            from_table=self._from_table,
            from_subquery=self._from_subquery,
            joins=self._joins,
            where_conditions=self._where_conditions,
            where_groups=self._where_groups,  # Added this line
            group_by=self._group_by,
            order_by=self._order_by,
            limit=self._limit,
            offset=self._offset
        )

        # Generate SQL
        generator = SQLGenerator(dialect="snowflake")
        sql, params = generator.generate(analyzed_query)

        return sql, params

    # pyquerybuilder/core/builder.py
    # Add or update these methods

    def from_(self, source) -> 'QueryBuilder':
        """Set the source for the FROM clause, which can be a table or subquery.

        Args:
            source: Table name, subquery, or QueryBuilder instance

        Returns:
            Self for method chaining
        """
        from .subquery import Subquery

        # Handle QueryBuilder as subquery
        if isinstance(source, QueryBuilder) and not isinstance(source, Subquery):
            return self.from_subquery(source)

        # Handle existing Subquery object
        elif hasattr(source, 'get_sql'):
            return self.from_subquery(source)

        # Handle table name (string)
        else:
            return self.from_table(source)

    from ..query.where_group import WhereGroup

    # Add these methods to the QueryBuilder class
    def where(self, field, operator=None, value=None):
        """Add a WHERE condition to the query.

        This method can be called in three ways:
        1. where(field, operator, value)
        2. where(field, value) - with implied "=" operator
        3. where(where_group) - with a WhereGroup object

        Args:
            field: Field name, function, or WhereGroup
            operator: Comparison operator or value (if value is None)
            value: Value to compare against

        Returns:
            Self for method chaining
        """
        # Handle WhereGroup
        if isinstance(field, WhereGroup):
            self._where_groups.append(field)
            return self

        # Handle normal condition
        if value is None and operator is not None:
            # Shift parameters for convenience
            value = operator
            operator = "="

        self._where_conditions.append({
            "field": field,
            "operator": operator,
            "value": value
        })
        return self

    def or_where(self, field, operator=None, value=None):
        """Add a WHERE condition with OR logic.

        Args:
            field: Field name, function, or WhereGroup
            operator: Comparison operator or value (if value is None)
            value: Value to compare against

        Returns:
            Self for method chaining
        """
        # Handle WhereGroup
        if isinstance(field, WhereGroup):
            field._is_or = True
            self._where_groups.append(field)
            return self

        # Handle normal condition
        if value is None and operator is not None:
            # Shift parameters for convenience
            value = operator
            operator = "="

        self._where_conditions.append({
            "field": field,
            "operator": operator,
            "value": value,
            "logic": "OR"
        })
        return self

    def where_in(self, field, values):
        """Add a WHERE IN condition.

        Args:
            field: Field name or function
            values: List of values or subquery

        Returns:
            Self for method chaining
        """
        self._where_conditions.append({
            "field": field,
            "operator": "IN",
            "value": values
        })
        return self

    def where_not_in(self, field, values):
        """Add a WHERE NOT IN condition.

        Args:
            field: Field name or function
            values: List of values or subquery

        Returns:
            Self for method chaining
        """
        self._where_conditions.append({
            "field": field,
            "operator": "NOT IN",
            "value": values
        })
        return self

    def where_between(self, field, start, end):
        """Add a WHERE BETWEEN condition.

        Args:
            field: Field name or function
            start: Lower bound
            end: Upper bound

        Returns:
            Self for method chaining
        """
        self._where_conditions.append({
            "field": field,
            "operator": "BETWEEN",
            "value": (start, end)
        })
        return self

    def where_not_between(self, field, start, end):
        """Add a WHERE NOT BETWEEN condition.

        Args:
            field: Field name or function
            start: Lower bound
            end: Upper bound

        Returns:
            Self for method chaining
        """
        self._where_conditions.append({
            "field": field,
            "operator": "NOT BETWEEN",
            "value": (start, end)
        })
        return self

    # pyquerybuilder/core/builder.py
    # Add these imports at the top
    from .set_operation import SetOperation, SetOperationType

    # Add these methods to the QueryBuilder class

    def union(self, other_query):
        """Create a UNION with another query.

        Args:
            other_query: Another QueryBuilder instance

        Returns:
            SetOperation object
        """
        return SetOperation(self, SetOperationType.UNION, other_query)

    def union_all(self, other_query):
        """Create a UNION ALL with another query.

        Args:
            other_query: Another QueryBuilder instance

        Returns:
            SetOperation object
        """
        return SetOperation(self, SetOperationType.UNION_ALL, other_query)

    def intersect(self, other_query):
        """Create an INTERSECT with another query.

        Args:
            other_query: Another QueryBuilder instance

        Returns:
            SetOperation object
        """
        return SetOperation(self, SetOperationType.INTERSECT, other_query)

    def except_(self, other_query):
        """Create an EXCEPT with another query.

        Args:
            other_query: Another QueryBuilder instance

        Returns:
            SetOperation object
        """
        return SetOperation(self, SetOperationType.EXCEPT, other_query)

    def minus(self, other_query):
        """Create a MINUS with another query (Oracle's EXCEPT).

        Args:
            other_query: Another QueryBuilder instance

        Returns:
            SetOperation object
        """
        return SetOperation(self, SetOperationType.MINUS, other_query)




