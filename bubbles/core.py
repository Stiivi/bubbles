# -*- Encoding: utf8 -*-
"""DataObject operations"""

from .errors import *
from .objects import *
from .extensions import get_namespace, collect_subclasses
from .common import get_logger
from collections import defaultdict, namedtuple, UserList

import itertools
import inspect

__all__ = (
            "Signature",
            "OperationContext",
            "Operation",
            "operation",
            "is_operation",

            "common_representations",
            "extract_signatures",

            "default_context",

            "LoggingContextObserver",
            "CollectingContextObserver",
        )

"""List of modules with operations to be loaded for the default contex."""
# FIXME: make this more intelligent and lazy
_default_op_modules = (
            "bubbles.backends.sql.ops",
            "bubbles.iterator",
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
        if isinstance(signature, (list, tuple)):
            self.signature = Signature(*signature)
        else:
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
            op = Operation(fn, signature=signature, name=name)
        return op

    return decorator

class OperationList(UserList):
    def __init__(self):
        super().__init__()
        self.argument_count = None
        self.prototype_sitnature = None

    def append(self, op):
        """Appends op to the operation list. If the `op` is first operation,
        then treat it as prototype and set required operation argument
        count."""

        if self.argument_count is None:
            self.set_prototype(op)
        if len(op.signature) != self.argument_count:

            raise ArgumentError("Number of object arguments (%s) for %s do not"
                    "match prototype (%s)" % (len(op.signature, op,
                                              self.argument_count)))

        super().append(op)

    def set_prototype(self, op):
        """Sets operation prototype for this operation list."""
        self.argument_count = len(op.signature)
        self.prototype_signature = op.signature.as_prototype()


class OperationContext(object):
    # TODO: add parent_context
    def __init__(self, retry_count=10):
        """Creates an operation context. Default `retry_count` is 10.

        Use:

        .. code-block:: python

            c = Context()
            duplicate_records = c.duplicates(table)

        Context uses multiple dispatch based on operation arguments. The
        `k.duplicates()` operation might be different, based on available
        representations of `table` object.
        """

        super().__init__()
        self.operations = defaultdict(OperationList)
        self.operation_retry_count = retry_count
        self.o = _OperationGetter(self)

        self.logger = get_logger()
        self.observer = LoggingContextObserver(self.logger)
        self.retry_count = retry_count

    def operation_list(self, name):
        if name not in self.operations:
            self.operation_not_found(name)

        try:
            ops = self.operations[name]
        except KeyError:
            raise OperationError("Unable to find operation %s" % name)

        return ops

    def operation_prototype(self, name):
        """Returns a prototype signature for operation `name`"""
        oplist = self.operation_list(name)
        return oplist.prototype_signature

    def add_operations_from(self, obj):
        """Import operations from `obj`. All attributes of `obj` are
        inspected. If they are instances of Operation, then the operation is
        imported."""

        for name in dir(obj):
            op = getattr(obj, name)
            if isinstance(op, Operation):
                self.add_operation(op)

    def add_operation(self, op):
        """Registers a decorated operation.  operation is considered to be a
        context's method and would receive the context as first argument."""

        if op.name not in self.operations:
            self.operations[op.name] = OperationList()

        if any(c.signature == op.signature for c in self.operations[op.name]):
            raise ArgumentError("Operation %s with signature %s already "
                                "registered" % (op.name, op.signature))

        self.operations[op.name].append(op)

    def remove_operation(self, name, signature=None):
        """Removes all operations with `name` and `signature`. If no
        `signature` is specified, then all operations with given name are
        removed, regardles of the signature."""

        operations = self.operations.get(name)
        if not operations:
            return
        elif not signature:
            del self.operations[name]
            return

        newops = [op for op in operations if op.signature != signature]
        self.operations[name] = newops

    def operation_list(self, name):
        """Return a list of operations with `name`. If no list is found, then
        `operation_not_found(name)` is called. See `operation_not_found` for
        more information"""
        # Get all signatures registered for the operation
        operations = self.operations[name]
        if not operations:
            self.operation_not_found(name)
            operations = self.operations[name]
            if not operations:
                raise OperationError("Unable to find operation %s" % name)

        return operations

    def operation_not_found(self, name):
        """Subclasses might override this method to load necessary modules and
        register operations using `register_operation()`. Default
        implementation raises an exception."""
        raise OperationError("Operation '%s' not found" % name)

    def get_operation(self, name, signature):
        """Returns an operation with given name and signature. Requires exact
        signature match."""
        # Get all signatures registered for the operation
        operations = self.operation_list(name)

        for op in operations:
            if op.signature == signature:
                return op
        raise OperationError("No operation '%s' with signature '%s'" %
                                                        (name, signature))

    def operation(self, name):
        """Returns a bound operation with `name`"""

        return _OperationReference(self, name)

    def debug_print_catalogue(self):
        """Pretty prints the operation catalogue – all operations, their
        signatures and function references. Used for debugging purposes."""

        print("== OPERATIONS ==")
        keys = list(self.operations.keys())
        keys.sort()
        for name in keys:
            ops = self.operations[name]
            print("* %s:" % name)

            for op in ops:
                print("    - %s:%s" % \
                            (op.signature.signature, op.function) )

    def call(self, op_name, *args, **kwargs):
        """Call operation with `name`. Arguments are passed to the
        operation"""

        oplist = self.operation_list(op_name)
        argc = oplist.argument_count
        match_objects = args[0:argc]

        # Find best signature match for the operation based on object
        # arguments
        op = self.lookup_operation(op_name, *match_objects)

        if self.observer:
            self.observer.will_call_operation(self, op)

        result = None
        retries = 0

        # We try to perform requested operation. If the operation raises
        # RetryOperation exception, then we use signature from the exception
        # for another operation. Operation can be retried retry_count number
        # of times.
        # Observer is notified about each retry.

        # TODO: allow retry without a signature
        # This will require to have a prepared list of matching operations and
        # number of retries will be number of operations in the list.

        try:
            result = op.function(self, *args, **kwargs)
        except RetryOperation as e:
            signature = e.signature
            reason = e.reason
            success = False

            for i in range(0, self.retry_count):
                op = self.get_operation(op_name, signature)
                retries += 1

                if self.observer:
                    self.observer.will_retry_operation(self, op, reason)

                try:
                    result = op.function(self, *args, **kwargs)
                except RetryOperation as e:
                    signature = e.signature
                    reason = e.reason
                else:
                    success = True
                    break

            if not success:
                raise RetryError("Operation retry limit reached "
                                     "(allowed: %d)" % self.retry_count)

        # Let the observer know which operation was called at last and
        # completed sucessfully

        if self.observer:
            self.observer.did_call_operation(self, op, retries)

        return result

    def lookup_operation(self, name, *objlist):
        """Returns a matching operation for given data objects as arguments.
        This is the main lookup method of the operation context.

        Lookup is performed for all representations of objects, the first
        matching signature is accepted. Order of representations returned by
        object's `representations()` matters here.

        Returned object is a OpFunction tuple with attributes: `name`, `func`,
        `signature`.

        Note: If the match does not fit your expectations, it is recommended
        to pefrom explicit object conversion to an object with desired
        representation.
        """

        operations = self.operation_list(name)
        if not operations:
            # TODO: check parent context
            raise OperationError("No known signatures for operation '%s'" %
                                    name)

        # TODO: add some fall-back mechanism when no operation signature is
        # found
        # Extract signatures from arguments

        arg_signatures = extract_signatures(*objlist)
        match = None
        for arguments in itertools.product(*arg_signatures):
            for op in operations:
                if op.signature.matches(*arguments):
                    match = op
                    break
            if match:
                break

        if match:
            return match

        raise OperationError("No matching signature found for operation '%s' "
                             " (args: %s)" %
                                (name, arg_signatures))

class LoggingContextObserver(object):
    def __init__(self, logger=None):
        self.logger = logger

    def will_call_operation(self, ctx, op):
        logger = self.logger or ctx.logger
        logger.debug("calling %s(%s)" % (op, op.signature))

    def did_call_operation(self, ctx, op, retries):
        logger = self.logger or ctx.logger

        if not retries:
            logger.info("called %s(%s)" % (op, op.signature))
        else:
            logger.info("called %s(%s) wth %s retries" % \
                                (op, op.signature, retries))

    def will_retry_operation(self, ctx, op, reason):
        logger = self.logger or ctx.logger
        logger.debug("retry %s(%s), reason: %s" %
                                        (op, op.signature, reason))

class CollectingContextObserver(object):
    def __init__(self):
        self.history = []
        self.current = None
        self.tried = []

    def will_call_operation(self, ctx, op):
        self.current = op
        self.tried = []

    def will_retry_operation(self, ctx, op, reason):
        self.tried.append( (self.current, reason) )
        self.current = op

    def did_call_operation(self, ctx, op, retries):
        line = (op, retries, self.tried)
        self.history.append(line)


class _OperationGetter(object):
    def __init__(self, context):
        self.context = context

    def __getattr__(self, name):
        return self.context.operation(name)


class _OperationReference(object):
    def __init__(self, context, name):
        """Creates a reference to an operation within a context"""
        self.context = context
        self.name = name

    def __call__(self, *args, **kwargs):
        return self.context.call(self.name, *args, **kwargs)


class _DefaultContext(OperationContext):
    def operation_not_found(self, name):
        """Load default modules"""
        for modname in _default_op_modules:
            mod = _load_module(modname)
            self.add_operations_from(mod)

        self.operation_not_found = super().operation_not_found


def _load_module(modulepath):
    mod = __import__(modulepath)
    path = []
    for token in modulepath.split(".")[1:]:
       path.append(token)
       try:
           mod = getattr(mod, token)
       except AttributeError:
           raise BubblesError("Unable to get %s" % (path, ))
    return mod


default_context = _DefaultContext()
# TODO: k is for compatibility reasons and means 'kernel'
k = default_context

if __name__ == "__main__":
    k.load_default_operations()
    k.debug_print_catalogue()

