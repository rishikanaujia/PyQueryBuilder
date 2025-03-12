# Running PyQueryBuilder

To run the PyQueryBuilder project, follow these steps:

## 1. Set Up Your Environment

First, create and activate a virtual environment:

```bash
# Create virtual environment
python -m venv venv

# Activate on Windows
venv\Scripts\activate

# Activate on macOS/Linux
source venv/bin/activate
```

## 2. Install Dependencies

Install the required dependencies:

```bash
pip install snowflake-connector-python
```

## 3. Organize Project Files

Make sure all the files are organized according to the structure we've defined:

```
pyquerybuilder/
├── __init__.py
├── core/
│   ├── __init__.py
│   ├── builder.py
│   └── executor.py
├── discovery/
│   ├── __init__.py
│   ├── metadata_inspector.py
│   └── snowflake/
│       ├── __init__.py
│       ├── connector.py
│       └── schema_reader.py
# ... and so on
```

## 4. Create a Test Script

Create a test script (e.g., `test_query.py`) in your project root directory:

```python
from pyquerybuilder import QueryBuilder

# Initialize with Snowflake connection
builder = QueryBuilder.from_snowflake(
    account='your_account',
    user='your_username',
    password='your_password',
    warehouse='your_warehouse',
    database='your_database',
    schema='your_schema'
)

# Build a query
query = (
    builder
    .select("tr.transactionId", "targetcompany.companyName as Target")
    .from_table("ciqTransaction as tr")
    .join("ciqCompany as targetcompany")
    .where("tr.announcedYear", ">=", 2020)
    .order_by("tr.announcedYear", "desc")
)

# Build and print the SQL
sql, params = query.build()
print("Generated SQL:")
print(sql)
print("\nParameters:")
print(params)

# Execute the query
try:
    results = query.execute()
    print(f"\nFound {len(results)} results")
    for i, row in enumerate(results[:5]):
        print(f"Row {i+1}:", row)
except Exception as e:
    print(f"Error executing query: {e}")
```

## 5. Run the Test Script

```bash
python test_query.py
```

## Troubleshooting

1. **Import Errors**: Make sure your project structure is correct and that the directory containing `pyquerybuilder` is in your Python path.

2. **Connection Issues**: Verify your Snowflake connection details are correct.

3. **Schema Discovery Errors**: The auto-discovery may fail if your Snowflake account doesn't have the necessary permissions or if the schema structure is unusual.

4. **SQL Generation Issues**: If the SQL isn't generated correctly, you might need to adjust the field analyzer or join resolver.

## Development Tips

1. **Start small**: Begin with simple queries to verify the core functionality.

2. **Add logging**: Insert print statements to debug the internal state during development.

3. **Incremental testing**: Test each component (discovery, analysis, SQL generation) separately.

4. **Check Snowflake documentation**: Make sure your queries adhere to Snowflake SQL syntax.

This basic implementation should get you started with PyQueryBuilder. As you use it, you can expand and improve the functionality based on your specific needs.