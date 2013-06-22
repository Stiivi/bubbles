# -*- Encoding: utf8 -*-
"""Various utility functions"""

import datetime

__all__ = (
        "expand_record",
        "collapse_record",
        "guess_type"
        )


def guess_type(string, date_format="%Y-%m-%dT%H:%M:%S.Z"):
    """Guess one of basic types that the `string` might contain. Returns a
    string with basic type name. If `date_format` is ``None`` then string is
    not tested for date type. Default is ISO date format."""

    if string is None:
        return None

    try:
        int(string)
        return "integer"
    except ValueError:
        pass

    try:
        float(string)
        return "float"
    except ValueError:
        pass

    if date_format:
        try:
            datetime.datetime.strptime(string, date_format)
            return "date"
        except ValueError:
            pass

    return "string"


def expand_record(record, separator = '.'):
    """Expand record represented as dict object by treating keys as key paths separated by
    `separator`, which is by default ``.``. For example: ``{ "product.code": 10 }`` will become
    ``{ "product" = { "code": 10 } }``

    See :func:`bubbles.collapse_record` for reverse operation.
    """
    result = {}
    for key, value in list(record.items()):
        current = result
        path = key.split(separator)
        for part in path[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
        current[path[-1]] = value
    return result


def collapse_record(record, separator = '.', root = None):
    """See :func:`bubbles.expand_record` for reverse operation.
    """

    result = {}
    for key, value in list(record.items()):
        if root:
            collapsed_key = root + separator + key
        else:
            collapsed_key = key

        if type(value) == dict:
            collapsed = collapse_record(value, separator, collapsed_key)
            result.update(collapsed)
        else:
            result[collapsed_key] = value
    return

