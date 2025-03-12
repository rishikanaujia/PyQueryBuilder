# pyquerybuilder/sql/functions/__init__.py
"""SQL functions for PyQueryBuilder."""

from .aggregate_functions import Sum, Count, Avg, Min, Max
from .date_functions import DatePart, DateTrunc, CurrentDate
from .string_functions import Concat, Upper, Lower, Substring


# Create a namespace for functions
class Functions:
    """Namespace for SQL functions."""
    # Aggregate
    Sum = Sum
    Count = Count
    Avg = Avg
    Min = Min
    Max = Max

    # Date
    DatePart = DatePart
    DateTrunc = DateTrunc
    CurrentDate = CurrentDate

    # String
    Concat = Concat
    Upper = Upper
    Lower = Lower
    Substring = Substring


# Export as 'fn' for convenience
fn = Functions