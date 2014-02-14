# -*- Encoding: utf8 -*-
"""DataObject operations"""

from .errors import *
from .objects import *
from .extensions import collect_subclasses
from .common import get_logger
from collections import defaultdict, namedtuple, UserList
from .dev import is_experimental

import itertools
import inspect

__all__ = (
            "Signature",
            "Operation",
            "operation",
            "is_operation",
            "common_representations",
            "extract_signatures",
        )

Operand = namedtuple("Operand", ["rep", "islist", "isany"])


OperationPrototype = namedtuple("OperationPrototype",
                                [   "name",
                                    "operand_count",
                                    "operands",
                                    "parameters"])


def rep_to_operand(rep):
    """Converts representation to `Operand` definition"""

    if rep.endswith("[]"):
        optype = rep[:-2]
        islist = True
    else:
        optype = rep
        islist = False

    isany = (optype == "*")

    return Operand(optype, islist, isany)


class Signature(object):
    def __init__(self, *signature):
        """Creates an operation signature. Sitnature is a list of object
        representations for operation arguments.

        * An unary operation on a SQL object has signature: `Signature("sql")`
        * Binary operation on two iterators has signature:
          `Signature("rows", "rows")`
        * Operation on a list of iterators, such as operation that appends
          multiople iterators, has signature `Signature("rows[]")`
        * Operation that accepts any kind of object: `Signature("*")`
        * Operation that accepts a list of any objects: `Signature("*[]")`
        """

        self.signature = tuple(signature)
        self.operands = tuple(rep_to_operand(rep) for rep in signature)

    def __getitem__(self, index):
        return self.signature[index]

    def __len__(self):
        return len(self.signature)

    def __eq__(self, obj):
        """Signatures can be compared to lists or tuples of strings"""

        if isinstance(obj, Signature):
            return self.signature == obj.signature
        elif isinstance(obj, (list, tuple)):
            return self.signature == tuple(obj)
        else:
            return False

    def __ne__(self, obj):
        return not self.__eq__(obj)

    def __repr__(self):
        args = ", ".join(self.signature)

        return "Signature(%s)" % args

    def __str__(self):
         return ", ".join(self.signature)

    def matches(self, *operands):
        """Returns `True` if the signature matches signature of `operands`.
        `operands` is a list of strings.

        Rules:
            * ``rep`` matches ``rep`` and ``*``
            * ``rep[]`` matches ``rep[]`` and ``*[]``

        Example matches:

            * `Signature("sql")` matches ``"sql")``
            * `Signature("*")` matches ``"sql"``
            * `Signature("sql[]")` matches ``"sql[]"``
            * `Signature("*[]")` matches ``"sql[]"``

            * `Signature("sql")` does not match ``"rows"``
            * `Signature("sql")` does not match ``"sql[]"``
        """

        if len(operands) != len(self.operands):
            return False

        operands = [rep_to_operand(rep) for rep in operands]

        for mine, their in zip(self.operands, operands):
            if mine.islist != their.islist:
                return False
            if not mine.isany and mine.rep != their.rep:
                return False

        return True

    def __hash__(self):
        return hash(self.signature)

    def description(self):
        return {"args":[str(s) for s in self.signature]}

    def as_prototype(self):
        """Returns a `Signature` object that serves as a prototype for similar
        signatures. All representations in the prototype signature are set to
        `any`, number of arguments is preserved."""

        sig = []

        for op in self.operands:
            if op.islist:
                sig.append("*[]")
            else:
                sig.append("*")
        return Signature(*sig)


def common_representations(*objects):
    """Return list of common representations of `objects`"""

    common = list(objects[0].representations())
    for obj in objects[1:]:
        common = [rep for rep in common if rep in obj.representations()]

    return common

def extract_signatures(*objects):
    """Extract possible signatures."""

    signatures = []
    # Get representations of objects
    for obj in objects:
        if isinstance(obj, DataObject):
            signatures.append(obj.representations())
        elif isinstance(obj, (list, tuple)):
            common = common_representations(*obj)
            common = [sig + "[]" for sig in common]
            signatures.append(common)
        else:
            raise ArgumentError("Unknown type of operation argument "\
                                "%s (not a data object)" % type(obj).__name__)
    return signatures

class Operation(object):
    def __init__(self, func, signature, name=None):
        """Creates an operation with function `func` and `signature`. If
        `name` is not specified, then function name is used."""

        pysig = inspect.signature(func)
        if len(pysig.parameters) < len(signature) + 1:
            raise ArgumentError("Function %s does not have sufficient number of parameters "
                              "for signature %s. Maybe context parameter "
                              "missing?" % (func, signature))

        self.function = func
        self.name = name or self.function.__name__
        self.signature = signature

    def __eq__(self, other):
        if not isinstance(other, Operation):
            return False
        return other.function == self.function \
                and other.name == self.name \
                and other.signature == self.signature

    def __call__(self, *args, **kwargs):
        self.function(*args, **kwargs)

    def description(self):
        """Provide a dictionary with operation description. Useful for
        end-user applications or providing human readable inspection."""
        d = {
            "name": self.name,
            "doc": func.__doc__,
            "signature": signature.description()
        }

    def __str__(self):
        return self.name

def is_operation(function):
    """Returns `True` if `function` is decorated operation."""
    return isinstance(function, Operation)


def operation(*signature, name=None):
    """Decorates the function to be an operation"""

    def decorator(fn):
        if is_operation(fn):
            raise ArgumentError("Function %s is already an operation (%s)" %
                                  (fn.function.__name__, fn.name))
        else:
            op = Operation(fn, signature=Signature(*signature), name=name)
        return op

    return decorator

class OperationList(UserList):
    def __init__(self):
        super().__init__()
        self.prototype = None

    def append(self, op):
        """Appends op to the operation list. If the `op` is first operation,
        then treat it as prototype and set required operation argument
        count."""

        if not self.prototype:
            self.set_prototype(op)
        if len(op.signature) != self.prototype.operand_count:
            raise ArgumentError("Number of object arguments (%s) for %s do not"
                    "match prototype (%s)" % (len(op.signature), op,
                                              self.prototype.operand_count))

        super().append(op)

    def set_prototype(self, op):
        """Sets operation prototype for this operation list."""
        opcount = len(op.signature)

        function_sig = inspect.signature(op.function)
        # Extract just parameter names (sig.parameters is a mapping)
        names = tuple(function_sig.parameters.keys())
        # Set operand names from function parameter names, skip the context
        # parameter and use only as many parameters as operands in the
        # signature
        operands = names[1:1+opcount]
        # ... rest of the names are considered operation parameters
        parameters = names[1+opcount:]

        self.prototype = OperationPrototype(
                    op.name,
                    opcount,
                    operands,
                    parameters
                )


