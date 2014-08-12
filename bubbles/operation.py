# -*- Encoding: utf8 -*-
"""DataObject operations"""

from .errors import *
from .objects import *
from .extensions import collect_subclasses
from .common import get_logger
from collections import OrderedDict, namedtuple
from .dev import is_experimental

import itertools
import inspect

__all__ = (
            "Signature",
            "Operation",
            "operation",
            "common_representations",
            "get_representations"
        )

Operand = namedtuple("Operand", ["rep", "islist", "isany"])

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

    def has_any(self):
        """Returns `True` if at least one operand is `any` (``*``)"""
        return any(op.isany for op in self.operands)

    # TODO: DEPRECIATE!
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

def get_representations(*operands):
    """For every operand get list of it's representations. Returns list of
    lists."""

    reps = []
    # Get representations of objects
    for obj in operands:
        if hasattr(obj, "representations"):
            reps.append(obj.representations())
        elif isinstance(obj, (list, tuple)):
            common = common_representations(*obj)
            common = [sig + "[]" for sig in common]
            reps.append(common)
        else:
            raise ArgumentError("Unknown type of operation argument "\
                                "%s (not a data object)" % type(obj).__name__)

    return reps

class Operation(object):
    def __init__(self, name, operands=None, parameters=None):
        """Creates an operation with name `name` and `operands`. If `operands`
        is `none`, then one operand is assumed with name `obj`.

        The `func` should have a signature compatible with `function(context,
        *operands, **parameters)`
        """

        self.name = name

        if operands is None:
            operands = ["obj"]

        if not operands:
            raise ArgumentError("Operand list sohuld not be empty.")

        self.operands = operands
        self.opcount = len(operands)
        self.parameters = parameters

        self.registry = OrderedDict()

        self.experimental = False

    def __eq__(self, other):
        if not isinstance(other, Operation):
            return False
        return other.name == self.name \
                and other.registry == self.registry

    def signatures(self):
        """Return list of registered signatures."""
        return list(self.registry.keys())

    def function(self, signature):
        """Returns a function for `signature`"""
        return self.registry[signature]

    def resolution_order(self, representations):
        """Returns ordered list of signatures for `operands`. The generic
        signatures (those containing at least one ``*``/`any` type are placed
        at the end of the list.

        Note: The order of the generics is undefined."""

        # TODO: make the order well known, for example by having signatures
        # sortable
        generics = []
        signatures = []

        for sig in self.signatures():
            if sig.has_any():
                generics.append(sig)
            else:
                signatures.append(sig)

        matches = []
        gen_matches = []
        for repsig in itertools.product(*representations):
            matches += [sig for sig in signatures if sig.matches(*repsig)]
            gen_matches += [sig for sig in generics if sig.matches(*repsig)]

        matches += gen_matches

        if not matches:
            raise OperationError("No matching signature found for operation '%s' "
                                 " (args: %s)" %
                                    (self.name, representations))
        return matches


    def register(self, *signature, name=None):
        sig = None

        def register_function(func):
            nonlocal sig
            nonlocal name

            # TODO Test for non-keyword arguments for better error reporting
            func_sig = inspect.signature(func)
            if len(func_sig.parameters) < (1 + self.opcount):
                raise ArgumentError("Expected at least %d arguments in %s. "
                                    "Missing context argument?"
                                    % (self.opcount + 1, func.__name__))

            self.registry[sig] = func
            func.__name__ = self.name
            return func

        if signature and callable(signature[0]):
            func, *signature = signature

            # Create default signature if none is provided
            if not signature:
                signature = ["*"] * self.opcount

            sig = Signature(*signature)
            return register_function(func)
        else:
            sig = Signature(*signature)
            return register_function

    def __str__(self):
        return self.name


def operation(*args):
    """Creates an operation prototype. The operation will have the same name
    as the function. Optionaly a number of operands can be specified. If no
    arguments are given then one operand is assumed."""
    opcount = 1

    def decorator(func):
        nonlocal opcount

        # No arguments, this is the decorator
        # Set default values for the arguments
        # Extract just parameter names (sig.parameters is a mapping)
        sig = inspect.signature(func)
        names = tuple(sig.parameters.keys())
        # Set operand names from function parameter names, skip the context
        # parameter and use only as many parameters as operands in the
        # signature
        operands = names[1:1 + opcount]
        # ... rest of the names are considered operation parameters
        parameters = names[1 + opcount:]
        op = Operation(func.__name__, operands, parameters)
        sig = ["*"] * len(operands)
        op.register(func, *sig)
        return op


    if len(args) == 1 and callable(args[0]):
        return decorator(args[0])
    else:
        # This is just returning the decorator
        opcount = args[0]
        return decorator



