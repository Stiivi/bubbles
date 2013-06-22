from ...errors import *
from ...metadata import Field, FieldList

__all__ = (
            "prepare_key",
            "zip_condition",
            "join_on_clause"
        )

def prepare_key(key):
    """Returns a list of columns for `key`. Key might be a list of fields or
    just one field represented by `Field` object or a string. Examples:

    >>> key = prepare_key(obj, "id")
    >>> key = prepare_key(obj, ["code", "name"])

    If `statement` is not `None`, then columns for the key are returned.
    """

    if isinstance(key, (str, Field)):
        key = (key, )

    return tuple(str(f) for f in key)


def join_on_clause(left, right, left_key, right_key):
    """Returns a conditional statement for joining `left` and `right`
    statements by keys."""

    left_cols = [left.c[str(key)] for key in left_key]
    right_cols = [right.c[str(key)] for key in right_key]

    return zip_condition(left_cols, right_cols)


def zip_condition(left, right):
    """Creates an equality condition by zipping `left` and `right` list of
    elements"""

    cond = [l == r for l, r in zip(left, right)]

    assert len(cond) >= 1
    if len(cond) > 1:
        cond = sql.expression.and_(*cond)
    else:
        cond = cond[0]

    return cond

