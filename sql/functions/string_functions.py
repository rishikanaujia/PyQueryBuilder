# pyquerybuilder/sql/functions/string_functions.py
"""String SQL functions for PyQueryBuilder."""
from typing import Any, Optional

from .base_function import Function


class Concat(Function):
    """Concatenate strings."""

    def __init__(self, *args, alias: Optional[str] = None):
        """Initialize CONCAT function."""
        super().__init__("CONCAT", *args, alias=alias)


class Upper(Function):
    """Convert string to uppercase."""

    def __init__(self, field: Any, alias: Optional[str] = None):
        """Initialize UPPER function."""
        super().__init__("UPPER", field, alias=alias)


class Lower(Function):
    """Convert string to lowercase."""

    def __init__(self, field: Any, alias: Optional[str] = None):
        """Initialize LOWER function."""
        super().__init__("LOWER", field, alias=alias)


class Substring(Function):
    """Extract a substring."""

    def __init__(self, field: Any, start: int, length: Optional[int] = None,
                 alias: Optional[str] = None):
        """Initialize SUBSTRING function."""
        if length is not None:
            super().__init__("SUBSTRING", field, start, length, alias=alias)
        else:
            super().__init__("SUBSTRING", field, start, alias=alias)