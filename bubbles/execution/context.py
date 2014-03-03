# -*- coding: utf-8 -*-
import itertools
from collections import defaultdict
from ..errors import *
from ..dev import is_experimental
from ..operation import Operation, Signature, get_representations
from ..common import get_logger
from ..threadlocal import LocalProxy

__all__ = (
            "OperationContext",
            "default_context",
            "LoggingContextObserver",
            "CollectingContextObserver",
        )

"""List of modules with operations to be loaded for the default contex."""

# FIXME: make this more intelligent and lazy
_default_op_modules = (
            "bubbles.backends.sql.ops",
            "bubbles.backends.mongo.ops",
            "bubbles.ops.rows",
            "bubbles.ops.generic",
        )


class _OperationReference(object):
    def __init__(self, context, name):
        """Creates a reference to an operation within a context"""
        self.context = context
        self.name = name

    def __call__(self, *args, **kwargs):
        return self.context.call(self.name, *args, **kwargs)


class OperationContext(object):
    # TODO: add parent_context
    def __init__(self, retry_count=10):
        """Creates an operation context. Default `retry_count` is 10.

        Use:

        .. code-block:: python

            c = Context()
            duplicate_records = c.op.duplicates(table)

        Context uses multiple dispatch based on operation arguments. The
        `k.duplicates()` operation might be different, based on available
        representations of `table` object.
        """

        super().__init__()
        self.operations = {}

        self.op = _OperationGetter(self)

        self.logger = get_logger()
        self.observer = LoggingContextObserver(self.logger)
        self.retry_count = retry_count

        self.retry_allow = []
        self.retry_deny = []

    def operation(self, name):
        """Get operation by `name`. If operatin does not exist, then
        `operation_not_found()` is called and the lookup is retried."""

        try:
            return self.operations[name]
        except KeyError:
            return self.operation_not_found(name)

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

        self.operations[op.name] = op

    def remove_operation(self, name):
        """Removes all operations with `name` and `signature`. If no
        `signature` is specified, then all operations with given name are
        removed, regardles of the signature."""

        del self.operations[name]

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

    def can_retry(self, opname):
        """Returns `True` if an operation can be retried. By default, all
        operations can be retried. `Context.retry_allow` contains list of
        allowed operations, `Context.retry_deny` contains list of denied
        operations. When the allow list is set, then only operations from the
        list are allowed. When the deny list is set, the operations in the
        deny list are not allowed."""

        if self.retry_deny and opname in self.retry_deny:
            return False

        if self.retry_allow and opname not in self.retry_allow:
            return False

        return True

    def call(self, op_name, *args, **kwargs):
        """Dispatch and call operation with `name`. Arguments are passed to the
        operation, If the operation raises `RetryOperation` then another
        function with signature from the exception is tried. If no signature
        is provided in the exception, then next matching signature is used."""

        op = self.operation(op_name)
        operands = args[:op.opcount]

        reps = get_representations(*operands)
        resolution_order = op.resolution_order(reps)
        first_signature = resolution_order[0]

        if self.observer:
            self.observer.will_call_operation(self, op, first_signature)

        result = None

        # We try to perform requested operation. If the operation raises
        # RetryOperation exception, then we use signature from the exception
        # for another operation. Operation can be retried retry_count number
        # of times.
        # Observer is notified about each retry.

        visited = set()

        while resolution_order:
            sig = resolution_order.pop(0)
            visited.add(sig)

            try:
                function = op.function(sig)
            except KeyError:
                raise OperationError("No signature (%s) in operation %s"
                                     % (sig, op_name))

            try:
                if op.experimental:
                    self.logger.warn("operation %s is experimental" % \
                                        op_name)
                result = function(self, *args, **kwargs)

            except RetryOperation as e:
                if not self.can_retry(op_name):
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

                elif retry:
                    retry = resolution_order[0]

                if self.observer:
                    self.observer.will_retry_operation(self, op, retry,
                                                       first_signature,
                                                       str(e))
            else:
                # Let the observer know which operation was called at last and
                # completed sucessfully
                if self.observer:
                    self.observer.did_call_operation(self, op, sig,
                                                     first_signature)
                return result

        raise RetryError("No remaining signature to rerty when calling "
                         "operation %s with %s"
                         % (op_name, first_signature))


class LoggingContextObserver(object):
    def __init__(self, logger=None):
        self.logger = logger

    def will_call_operation(self, ctx, op, signature):
        logger = self.logger or ctx.logger
        logger.info("calling %s(%s)" % (op, signature))

    def did_call_operation(self, ctx, op, signature, first):
        logger = self.logger or ctx.logger

        if first == signature:
            logger.debug("called %s(%s)" % (op, signature))
        else:
            logger.debug("called %s(%s) as %s" % \
                                (op, signature, first))

    def will_retry_operation(self, ctx, op, signature, first, reason):
        logger = self.logger or ctx.logger
        logger.info("retry %s(%s) as %s, reason: %s" %
                                        (op, first, signature, reason))

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


class BoundOperation(object):
    def __init__(self, context, opname):
        self.context = context
        self.opname = opname

    def __call__(self, *args, **kwargs):
        return self.context.call(self.opname, *args, **kwargs)


class _OperationGetter(object):
    def __init__(self, context):
        self.context = context

    def __getattr__(self, name):
        return BoundOperation(self.context, name)

    def __getitem__(self, name):
        return BoundOperation(self.context, name)


def create_default_context():
    """Creates a ExecutionContext with default operations."""
    context = OperationContext()

    for modname in _default_op_modules:
        mod = _load_module(modname)
        context.add_operations_from(mod)

    return context

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


default_context = LocalProxy("default_context",
                             factory=create_default_context)

