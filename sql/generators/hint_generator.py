# pyquerybuilder/sql/generators/hint_generator.py
"""Generator for query hints in SQL."""
from typing import List

from ..hints.base_hint import QueryHint


def generate_hints(hints: List[QueryHint]) -> str:
    """Generate SQL for query hints.

    Args:
        hints: List of QueryHint objects

    Returns:
        SQL string with hints
    """
    if not hints:
        return ""

    # Generate SQL for each hint
    hint_sql = " ".join(hint.get_sql() for hint in hints)

    return hint_sql