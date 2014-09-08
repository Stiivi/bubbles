# -*- Encoding: utf8 -*-

import inspect
from inspect import Parameter

from collections import namedtuple
from sys import intern
from functools import total_ordering

from .errors import ArgumentError


__all__ = (
    "Operation",
    "operation",
    "Signature",
    "to_type",
    "get_signature",
)

Operand = namedtuple("Operand", ["name", "type"])

Type = namedtuple("Type", ["specifier", "islist"])

ANY_SPECIFIER = "*"
Any = Type(ANY_SPECIFIER, False)

_registry = {}

def to_type(obj):
    """Returns a `Type` object for the `string`"""
    if obj is None:
        return obj
    elif isinstance(obj, Type):
        return obj
    elif obj.endswith("[]"):
        islist = True
        specifier = obj[:-2]
    else:
        islist = False
        specifier = obj

    return Type(specifier, islist)

def get_signature(fun):
    """Returns operation signature for callable `fun`.

        * first argument is considered to be a context argument
        * not annotated non-keyword arguments are considered as operands of
          default type
        * all annotated non-keyword arguments are considered as operands of
          the annotation type
        * operands should not have a default value set
    """

    pysig = inspect.signature(fun)

    if len(pysig.parameters) == 0:
        raise ArgumentError("Function '%s' has to have at least one argument"
                            " – execution context" % str(fun))

    parameters = list(pysig.parameters.values())
    operands = []
    for param in parameters[1:]:
        if param.kind != inspect.Parameter.POSITIONAL_OR_KEYWORD \
            or param.default != Parameter.empty:
            break

        if not param.annotation:
            raise ArgumentError("Operand can not be of type None. "
                                "Did you mean Any?")
        elif param.annotation == Parameter.empty:
            # Set to Any if not specified explicitly
            annotation = Any
        else:
            annotation = param.annotation

        operands.append(annotation)

    if pysig.return_annotation == Parameter.empty:
        return_annotation = None
    else:
        return_annotation = pysig.return_annotaton

    return Signature(*operands, output=return_annotation)


@total_ordering
class Signature(object):
    def __init__(self, *types, output=None):

        self.operands = tuple(to_type(t) for t in types)
        self.output = to_type(output)

    def match(self, *operand_types):
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

        if len(operand_types) != len(self.operands):
            return False

        types = [to_type(t) for t in operand_types]

        for mine, their in zip(self.operands, types):
            if mine.islist != their.islist \
                    or (mine.specifier != ANY_SPECIFIER \
                        and mine.specifier != their.specifier):
                return False

        return True

    def __hash__(self):
        return hash(self.operands)

    def __len__(self):
        """Lenght of the signature is number of the operands"""
        return len(self.operands)

    def __eq__(self, other):
        if not isinstance(other, Signature):
            return NotImplemented
        return other.operands == self.operands \
                and other.output == self.output

    def __lt__(self, other):
        if not hasattr(other, "operands"):
            return NotImplemented

        for mine, their in zip(self.operands, other.operands):
            if mine != Any and their == Any:
                return False

        return True

    def __repr__(self):
        args = ", ".join([op.specifier for op in self.operands])

        return "Signature(%s)" % args

    def __str__(self):
         return ", ".join([op.specifier for op in self.operands])


class Operation(object):
    def __init__(self, name):
        self.name = name

        self.prototype = None
        self.registry = dict()

    def register(self, *args):
        """Decorator that registers a function for this operation."""

        def register_function(func):
            sig = get_signature(func)

            if self.prototype and len(sig) != len(self.prototype):
                raise ArgumentError("The node '%s' expects %d operands "
                                    "in function '%s'"
                                    % (self.name, len(self.prototype), func))

            if self.prototype is None:
                ops = [Any] * len(sig)

                output = sig.output
                if output:
                    output = Any

                self.prototype = Signature(*ops, output=output)

            self.registry[sig] = func

            return func

        if args and callable(args[0]):
            # Nothing for the time being...
            func, *args = args

            return register_function(func)

        else:
            return register_function


    def __eq__(self, other):
        if not isinstance(other, Node):
            return False
        return other.name == self.name \
                and other.registry == self.registry

    @property
    def has_input(self):
        return len(self.prototype) > 0

    @property
    def has_output(self):
        return self.prototype.output is None

    @property
    def signatures(self):
        """Return list of registered signatures."""
        return list(self.registry.keys())

    @property
    def sorted_signatures(self):
        """Return list of registered signatures."""
        return sorted(self.registry.keys())

    def resolution_order(self, types):
        """Returns ordered list of signatures for `operands`. The generic
        signatures (those containing at least one ``*`` (`Any`) type are placed
        at the end of the list.

        Note: The order of the generics is undefined."""

        signatures = self.sorted_signatures
        matches = []

        for itertype in itertools.product(*types):
            matches += [sig for sig in signatures if sig.matches(*i)]

        if not matches:
            raise OperationError("No matching signature found for operation '%s' "
                                 " (args: %s)" %
                                    (self.name, types))
        return matches

class _OperationGetter(object):
    def __init__(self):
        pass

    def __getattr__(self, key):
        return _registry[key]


def get_operation(name):
    return _registry[name]

# TODO: this is temporary solution
def get_registry():
    return _registry

def operation(*args, **kwargs):
    """Creates an operation prototype. The operation will have the same name
    as the function. Optionaly a number of operands can be specified. If no
    arguments are given then one operand is assumed."""

    name = kwargs.get("name")

    def decorator(func):
        nonlocal name

        name = name or func.__name__
        try:
            op = _registry[name]
        except KeyError:
            op = Operation(name)
            _registry[name] = op

        op.register(func)
        return op

    if len(args) == 1 and callable(args[0]):
        return decorator(args[0])
    else:
        # This is just returning the decorator
        opcount = args[0]
        return decorator

