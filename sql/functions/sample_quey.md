# Example usage
from pyquerybuilder import QueryBuilder
from pyquerybuilder.sql.functions import fn

# Build a query with window functions
query = (
    builder
    .select(
        "companyName",
        "transactionSize",
        fn.Rank().over().partition_by("companyId").order_by("transactionSize", "DESC").as_("sizeRank"),
        fn.Lead("announcedDate", 1).over().partition_by("companyId").order_by("announcedDate").as_("nextDeal"),
        fn.Lag("transactionSize").over().partition_by("companyId").order_by("announcedDate").as_("prevDealSize")
    )
    .from_table("transactions")
    .order_by("companyName")
    .order_by("sizeRank")
)


query = (
    builder
    .select(
        "companyName",
        fn.RowNumber().over().partition_by("companyId").order_by("announcedDate").as_("dealSequence"),
        fn.Sum("transactionSize").over().partition_by("companyId").as_("companyTotal"),
        fn.Lag("transactionSize").over().order_by("announcedDate").as_("previousDealSize")
    )
    .from_table("transactions")
)