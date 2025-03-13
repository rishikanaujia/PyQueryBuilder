# pyquerybuilder/sql/generators/where_generator.py
"""Generator for WHERE clause in SQL queries."""
from typing import List, Dict, Any, Tuple

# pyquerybuilder/sql/generators/where_generator.py
"""Generator for WHERE clause in SQL queries with enhanced support."""
from typing import List, Dict, Any, Tuple, Union

from ...query.where_group import WhereGroup


def generate_where(conditions, where_groups=None, param_start_idx=0):
    """Generate a WHERE clause from conditions and condition groups.

    Args:
        conditions: List of basic condition dictionaries
        where_groups: List of WhereGroup instances
        param_start_idx: Starting index for parameters

    Returns:
        Tuple of (WHERE clause, parameters dict)
    """
    where_parts = []
    params = {}
    param_idx = param_start_idx

    # Process basic conditions
    for condition in conditions:
        condition_sql, condition_params, param_idx = _process_condition(
            condition, param_idx
        )
        where_parts.append(condition_sql)
        params.update(condition_params)

    # Process condition groups
    if where_groups:
        for group in where_groups:
            is_or = getattr(group, "_is_or", False)
            group_logic = "OR" if is_or else "AND"

            group_sql, group_params, param_idx = _process_where_group(
                group, param_idx
            )

            # Add the group with appropriate logic
            if where_parts and group_sql:
                where_parts.append(f"{group_logic} ({group_sql})")
            elif group_sql:
                where_parts.append(f"({group_sql})")

            params.update(group_params)

    # Build the complete WHERE clause
    if where_parts:
        where_clause = "WHERE " + " AND ".join(where_parts)
        return where_clause, params
    else:
        return "", {}


def _process_condition(condition, param_idx):
    """Process a single condition.

    Args:
        condition: Condition dictionary
        param_idx: Current parameter index

    Returns:
        Tuple of (condition SQL, parameters dict, new param_idx)
    """
    field = condition["field"]
    operator = condition["operator"]
    value = condition["value"]
    logic = condition.get("logic", "AND")

    # Parameters dictionary
    params = {}

    # Handle function objects in field
    if hasattr(field, 'get_sql'):
        field_sql = field.get_sql()
    else:
        field_sql = str(field)

    # Handle different operators
    if operator.upper() in ("IS NULL", "IS NOT NULL"):
        # No parameters needed for NULL checks
        condition_sql = f"{field_sql} {operator}"
    elif operator.upper() in ("IN", "NOT IN"):
        # Handle subquery in IN clause
        if hasattr(value, 'get_sql'):
            value_sql = value.get_sql()
            condition_sql = f"{field_sql} {operator} {value_sql}"
        # Handle list of values
        elif isinstance(value, (list, tuple, set)):
            placeholders = []
            for i, item in enumerate(value):
                param_name = f"p{param_idx}"
                param_idx += 1
                placeholders.append(f":{param_name}")
                params[param_name] = item

            values_str = ", ".join(placeholders)
            condition_sql = f"{field_sql} {operator} ({values_str})"
        else:
            # Single value in IN clause
            param_name = f"p{param_idx}"
            param_idx += 1
            params[param_name] = value
            condition_sql = f"{field_sql} {operator} (:{param_name})"
    elif operator.upper() in ("BETWEEN", "NOT BETWEEN"):
        # Handle BETWEEN with two parameters
        start, end = value

        start_param = f"p{param_idx}"
        param_idx += 1
        params[start_param] = start

        end_param = f"p{param_idx}"
        param_idx += 1
        params[end_param] = end

        condition_sql = f"{field_sql} {operator} :{start_param} AND :{end_param}"
    else:
        # Standard operators with one parameter
        # Handle function objects in value
        if hasattr(value, 'get_sql'):
            value_sql = value.get_sql()
            condition_sql = f"{field_sql} {operator} {value_sql}"
        else:
            param_name = f"p{param_idx}"
            param_idx += 1
            params[param_name] = value
            condition_sql = f"{field_sql} {operator} :{param_name}"

    return condition_sql, params, param_idx


def _process_where_group(group, param_idx):
    """Process a WhereGroup recursively.

    Args:
        group: WhereGroup instance
        param_idx: Current parameter index

    Returns:
        Tuple of (group SQL, parameters dict, new param_idx)
    """
    parts = []
    params = {}

    for item in group.conditions:
        item_type = item.get("type", "condition")

        if item_type in ("condition", "or_condition"):
            # Process simple condition
            condition_sql, condition_params, param_idx = _process_condition(
                item, param_idx
            )

            # Add with appropriate logic
            if parts and item_type == "or_condition":
                parts.append(f"OR {condition_sql}")
            else:
                parts.append(condition_sql)

            params.update(condition_params)
        elif item_type in ("and_group", "or_group"):
            # Process nested group
            nested_group = item["group"]
            group_sql, group_params, param_idx = _process_where_group(
                nested_group, param_idx
            )

            # Add with appropriate logic
            if parts and item_type == "or_group":
                parts.append(f"OR ({group_sql})")
            else:
                parts.append(f"({group_sql})")

            params.update(group_params)

    return " AND ".join(parts), params, param_idx

# def generate_where(conditions, param_start_idx=0):
#     """Generate a WHERE clause from conditions."""
#     if not conditions:
#         return "", {}
#
#     where_parts = []
#     params = {}
#     param_idx = param_start_idx
#
#     for condition in conditions:
#         field = condition["field"]
#         operator = condition["operator"]
#         value = condition["value"]
#
#         # Handle function objects in field
#         if hasattr(field, 'get_sql'):
#             field_sql = field.get_sql()
#         else:
#             field_sql = field
#
#         # Handle function objects in value
#         if hasattr(value, 'get_sql'):
#             value_sql = value.get_sql()
#             where_parts.append(f"{field_sql} {operator} {value_sql}")
#         # Handle null conditions
#         elif operator.upper() in ("IS NULL", "IS NOT NULL"):
#             where_parts.append(f"{field_sql} {operator}")
#         # Handle normal conditions with parameters
#         else:
#             param_name = f"p{param_idx}"
#             param_idx += 1
#             where_parts.append(f"{field_sql} {operator} :{param_name}")
#             params[param_name] = value
#
#     where_clause = "WHERE " + " AND ".join(where_parts)
#     return where_clause, params



# # pyquerybuilder/sql/generators/where_generator.py
# """Generator for WHERE clause in SQL queries."""
# from typing import List, Dict, Any, Tuple
#
#
# def generate_where(conditions, param_start_idx=0):
#     """Generate a WHERE clause from conditions.
#
#     Args:
#         conditions: List of condition dictionaries
#         param_start_idx: Starting index for parameters
#
#     Returns:
#         Tuple of (WHERE clause, parameters dict)
#     """
#     if not conditions:
#         return "", {}
#
#     where_parts = []
#     params = {}
#     param_idx = param_start_idx
#
#     for condition in conditions:
#         field = condition["field"]
#         operator = condition["operator"]
#         value = condition["value"]
#
#         # Parameter name
#         param_name = f"p{param_idx}"
#         param_idx += 1
#
#         # Handle the condition based on operator
#         if operator.upper() in ("IS NULL", "IS NOT NULL"):
#             where_parts.append(f"{field} {operator}")
#         else:
#             where_parts.append(f"{field} {operator} :{param_name}")
#             params[param_name] = value
#
#     where_clause = "WHERE " + " AND ".join(where_parts)
#     return where_clause, params