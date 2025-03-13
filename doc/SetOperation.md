The implementation of Set Operations Support is now complete with the changes we've made. You don't need to modify any other files since set operations function as standalone entities that build on top of existing query builders.

The key components we've created:

1. A `SetOperation` class to represent set operations between queries
2. A `SetOperationType` enum for the different types of operations (UNION, UNION ALL, INTERSECT, EXCEPT, MINUS)
3. Set operation methods in the `QueryBuilder` class
4. Exported these classes in the package's `__init__.py`

These changes enable users to write queries like:

```python
# Get active deals from this year and high-value deals from last year
current_deals = builder.select("*").from_table("deals").where("year", "=", 2023)
past_big_deals = builder.select("*").from_table("deals").where("year", "=", 2022).where("value", ">", 1000000)

# Combine the results
combined = current_deals.union_all(past_big_deals).order_by("date", "DESC")
```

The implementation is self-contained and works with the existing query building, analysis, and SQL generation components, so no additional changes are needed.

# Example usage
from pyquerybuilder import QueryBuilder

# Create first query
query1 = (
    builder
    .select("transactionId", "companyName", "transactionSize")
    .from_table("transactions")
    .where("announcedYear", "=", 2022)
)

# Create second query
query2 = (
    builder
    .select("transactionId", "companyName", "transactionSize")
    .from_table("transactions")
    .where("announcedYear", "=", 2021)
    .where("transactionSize", ">", 1000000)
)

# Combine with set operations
union_query = query1.union(query2)
union_all_query = query1.union_all(query2)
intersect_query = query1.intersect(query2)
except_query = query1.except_(query2)

# Add ORDER BY and LIMIT to the combined result
result = union_query.order_by("transactionSize", "DESC").limit(10)

# Execute the query
sql, params = result.build()