# pyquerybuilder/sql/functions/window_frames.py
"""Constants for window frame specifications."""

# Frame bounds
CURRENT_ROW = "CURRENT ROW"
UNBOUNDED_PRECEDING = "UNBOUNDED PRECEDING"
UNBOUNDED_FOLLOWING = "UNBOUNDED FOLLOWING"


def preceding(rows: int) -> str:
    """Create a PRECEDING frame bound.

    Args:
        rows: Number of rows preceding

    Returns:
        Frame bound expression
    """
    return f"{rows} PRECEDING"


def following(rows: int) -> str:
    """Create a FOLLOWING frame bound.

    Args:
        rows: Number of rows following

    Returns:
        Frame bound expression
    """
    return f"{rows} FOLLOWING"