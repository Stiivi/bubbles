# -*- Encoding: utf8 -*-

import inspect

from collections import namedtuple
from functools import total_ordering, wraps
from warnings import warn
from inspect import Parameter
from sys import intern

import itertools

from .metadata import FieldList
from .objects import IterableDataSource
from .errors import ArgumentError, RetryOperation, OperationError


__all__ = (
    "Operation",
    "operation",
    "Signature",
    "to_type",
    "get_signature",

    "datasource",
)

Operand = namedtuple("Operand", ["name", "type"])

Type = namedtuple("Type", ["specifier", "islist"])

ANY_SPECIFIER = "*"
Any = Type(ANY_SPECIFIER, False)

_default_library = {}

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


def common_representations(*objects):
    """Return list of common representations of `objects`"""

    common = list(objects[0].reps())
    for obj in objects[1:]:
        common = [rep for rep in common if rep in obj.reps()]

    return common


def get_representations(*args, **kwargs):
    """For every operand get list of it's representations. Returns list of
    lists."""

    reps = []
    # Get representations of objects
    for obj in args:
        if hasattr(obj, "reps"):
            reps.append(obj.reps())
        elif hasattr(obj, "representations"):
            warn("`representations` is depreciated in favor of `reps`",
                 DeprecationWarning)
            reps.append(obj.representations())
        elif isinstance(obj, (list, tuple)):
            common = common_representations(*obj)
            common = [sig + "[]" for sig in common]
            reps.append(common)
        else:
            raise ArgumentError("Unknown type of operation argument "\
                                "%s (not a data object)" % type(obj).__name__)

    return reps

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


# TODO: clean-up
def unary_generator(func):
    """Wraps a function that provides an operation returning an iterator.
    Assumes return fields are the same fields as first argument object"""
    @wraps(func)
    def decorator(ctx, obj, *args, **kwargs):
        result = func(ctx, obj, *args, **kwargs)
        return IterableDataSource(result, obj.fields.clone())

    return decorator


class Operation(object):
    def __init__(self, name):
        self.name = name

        self.prototype = None
        self.registry = dict()
        self.experimental = False

    def register(self, *args):
        """Decorator that registers a function for this operation."""

        def register_function(func):
            sig = get_signature(func)

            # Make the function return a real data object
            if inspect.isgeneratorfunction(func):
                func = unary_generator(func)

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
            matches += [sig for sig in signatures if sig.match(*itertype)]

        if not matches:
            raise OperationError("No matching signature found for operation '%s' "
                                 " (args: %s)" %
                                    (self.name, types))
        return matches


    def __call__(self, session, *args, **kwargs):
        return self.dispatch(session, *args, **kwargs)

    def dispatch(self, session, *args, **kwargs):
        """Dispatch the operation to appropriate function according to the
        arguments. If the function raises `RetryOperation` then another
        function in the execution order is tried."""

        opcount = len(self.prototype)

        operands = args[:opcount]

        reps = get_representations(*operands)
        resolution_order = self.resolution_order(reps)
        first_signature = resolution_order[0]

        session.logger.debug("op %s(%s)", self.name, reps)

        result = None

        # We try to perform requested operation. If the operation raises
        # RetryOperation exception, then we use signature from the exception
        # for another operation.

        # Observer is notified about each retry.

        visited = set()

        while resolution_order:
            sig = resolution_order.pop(0)
            visited.add(sig)

            try:
                function = self.registry[sig]
            except KeyError:
                raise OperationError("No signature (%s) in operation %s"
                                     % (sig, self))

            try:
                if self.experimental:
                    warn("operation {} is experimental".format(self.name),
                         FutureWarning)

                result = function(session, *args, **kwargs)

            except RetryOperation as e:
                if not session.can_retry(self.name):
                    raise RetryError("Retry of operation '%s' is not allowed")

                retry = e.signature

                if retry:
                    retry = Signature(*retry)

                    if retry in visited:
                        raise RetryError("Asked to retry operation %s with "
                                         "signature %s, but it was already "
                                         "visited."
                                         % (op_name, retry))

                    resolution_order.insert(0, retry)

                else:
                    retry = resolution_order[0]

            else:
                # Let the observer know which operation was called at last and
                # completed sucessfully
                session.logger.debug("Called %s(%s)", self.name, sig)
                return result

        raise RetryError("No remaining signature to rerty when calling "
                         "operation %s with %s"
                         % (op_name, first_signature))

class _OperationGetter(object):
    def __init__(self):
        pass

    def __getattr__(self, key):
        return _registry[key]


def get_operation(name):
    return _default_library[name]

# TODO: this is temporary solution
def get_default_library():
    return _default_library

def operation(*args, **kwargs):
    """Creates an operation prototype. The operation will have the same name
    as the function. Optionaly a number of operands can be specified. If no
    arguments are given then one operand is assumed."""

    name = kwargs.get("name")

    def decorator(func):
        nonlocal name

        name = name or func.__name__
        try:
            op = _default_library[name]
        except KeyError:
            op = Operation(name)
            _default_library[name] = op

        op.register(func)
        return op

    if len(args) == 1 and callable(args[0]):
        return decorator(args[0])
    else:
        # This is just returning the decorator
        return decorator


def datasource(*args, **kwargs):
    """Wraps a function as a datasource operation. Optional argument `fields`
    contains a `FieldList`-like object."""

    fields = kwargs.get("fields")
    name = kwargs.get("name")

    def decorator(func):
        nonlocal fields
        nonlocal name

        if fields:
            fields = FieldList(*fields)
        else:
            fields = FieldList(("data", "unknown"))

        @wraps(func)
        def wrapper(ctx, *args, **kwargs):
            result = func(ctx, *args, **kwargs)
            return IterableDataSource(result, fields)

        return operation(wrapper, name=name)

    if len(args) == 1 and callable(args[0]):
        return decorator(args[0])
    else:
        return decorator
