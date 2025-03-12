# PyQueryBuilder

A dynamic SQL query builder with automatic schema discovery for Snowflake.

## Overview

PyQueryBuilder is a Python library that simplifies writing SQL queries for Snowflake databases. It features automatic schema discovery, which means you don't need to manually define the database schema - PyQueryBuilder will detect tables, columns, and relationships automatically.

Key features:
- Automatic schema discovery from Snowflake metadata
- Intuitive fluent API for query building
- Automatic join resolution based on discovered relationships
- Type-safe query generation
- Minimal configuration required

## Installation

```bash
pip install pyquerybuilder
```

## Quick Start

```python
from pyquerybuilder import QueryBuilder

# Initialize with auto-discovered schema from Snowflake
builder = QueryBuilder.from_snowflake(
    account='your_account',
    user='your_username',
    password='your_password',
    warehouse='your_warehouse',
    database='your_database',
    schema='your_schema'  # Optional
)

# Build a query using the fluent API
query = (
    builder
    .select("tr.transactionId", "company.companyName as CompanyName")
    .from_table("ciqTransaction as tr")
    .join("ciqCompany as company")
    .where("tr.announcedYear", ">=", 2020)
    .order_by("tr.announcedYear", "desc")
    .limit(10)
)

# Execute the query
results = query.execute()

# Or get the SQL and parameters
sql, params = query.build()
print(sql)
```

## Features

### Auto-Discovery

PyQueryBuilder automatically discovers:
- Tables and views
- Columns and data types
- Primary and foreign key relationships
- Inferred relationships based on naming conventions

### Query Building

Build SQL queries with a clean, fluent API:
- SELECT with field selection and aliases
- FROM with table aliases
- JOINs with automatic resolution
- WHERE conditions with parameters
- GROUP BY and aggregation
- ORDER BY with sorting direction
- LIMIT and OFFSET for pagination

### Automatic Join Resolution

PyQueryBuilder will automatically determine the correct JOIN paths between tables, even for complex queries with multiple joins. You don't need to specify join conditions explicitly - the library will figure them out based on discovered relationships.

## Documentation

### Initializing with Snowflake

```python
builder = QueryBuilder.from_snowflake(
    account='your_account',
    user='your_username',
    password='your_password',
    warehouse='your_warehouse',
    database='your_database',
    schema='your_schema',  # Optional, defaults to PUBLIC
    include_tables=['table1', 'table2'],  # Optional, limit to specific tables
    exclude_tables=['audit_log'],  # Optional, exclude specific tables
    include_views=True  # Optional, include views in discovery
)
```

### Building Queries

```python
# Basic SELECT query
query = (
    builder
    .select("field1", "field2", "table.field3 as AliasName")
    .from_table("main_table")
)

# Joins
query = (
    builder
    .select("customer.name", "order.amount")
    .from_table("customers as customer")
    .join("orders as order")  # Join condition resolved automatically
)

# Filtering
query = (
    builder
    .select("field1", "field2")
    .from_table("table")
    .where("field1", "=", "value")
    .where("field2", ">", 100)
)

# Sorting and pagination
query = (
    builder
    .select("field1", "field2")
    .from_table("table")
    .order_by("field1", "asc")
    .order_by("field2", "desc")
    .limit(10)
    .offset(20)
)
```

### Executing Queries

```python
# Execute and get results
results = query.execute()

# Get SQL and parameters without executing
sql, params = query.build()
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.