# Example usage
from pyquerybuilder import QueryBuilder

# Create a CTE
summary_cte = (
    builder
    .select("companyId", "COUNT(*) as dealCount", "SUM(transactionSize) as totalValue")
    .from_table("transactions")
    .group_by("companyId")
)

# Use the CTE in the main query
query = (
    builder
    .with_(summary_cte, "company_summary")
    .select("c.companyName", "cs.dealCount", "cs.totalValue")
    .from_table("company_summary as cs")
    .join("companies as c")
    .where("cs.totalValue", ">", 1000000)
    .order_by("cs.totalValue", "DESC")
)

# Build and execute
sql, params = query.build()