# pyquerybuilder/core/builder.py
# Update the __init__ method

def __init__(self, schema_registry, connector=None):
    """Initialize the QueryBuilder with schema information."""
    self._schema_registry = schema_registry
    self._connector = connector

    # Query components
    self._select_fields = []
    self._from_table = None
    self._from_subquery = None
    self._joins = []
    self._where_conditions = []
    self._where_groups = []  # Added this line
    self._group_by = []
    self._order_by = []
    self._limit = None
    self._offset = None