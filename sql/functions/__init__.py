# pyquerybuilder/sql/functions/__init__.py
"""SQL functions for PyQueryBuilder."""

from .aggregate_functions import Sum, Count, Avg, Min, Max
from .date_functions import DatePart, DateTrunc, CurrentDate
from .string_functions import Concat, Upper, Lower, Substring
from .window_functions import (
    Rank, DenseRank, RowNumber, Lead, Lag,
    FirstValue, LastValue, NTile
)
from .window_frames import (
    CURRENT_ROW, UNBOUNDED_PRECEDING, UNBOUNDED_FOLLOWING,
    preceding, following
)


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

    # Window
    Rank = Rank
    DenseRank = DenseRank
    RowNumber = RowNumber
    Lead = Lead
    Lag = Lag
    FirstValue = FirstValue
    LastValue = LastValue
    NTile = NTile

    # Window frames
    CURRENT_ROW = CURRENT_ROW
    UNBOUNDED_PRECEDING = UNBOUNDED_PRECEDING
    UNBOUNDED_FOLLOWING = UNBOUNDED_FOLLOWING
    preceding = preceding
    following = following


# Export as 'fn' for convenience
fn = Functions