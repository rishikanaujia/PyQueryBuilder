# pyquerybuilder/sql/generators/order_generator.py
"""Generator for ORDER BY clause in SQL queries."""
from typing import List, Dict, Any


def generate_order_by(order_specs):
    """Generate an ORDER BY clause.

    Args:
        order_specs: List of dictionaries with field and direction

    Returns:
        ORDER BY clause string
    """
    if not order_specs:
        return ""

    order_parts = []

    for spec in order_specs:
        field = spec["field"]
        direction = spec.get("direction", "ASC").upper()

        # Validate direction
        if direction not in ("ASC", "DESC"):
            direction = "ASC"

        order_parts.append(f"{field} {direction}")

    return "ORDER BY " + ", ".join(order_parts)