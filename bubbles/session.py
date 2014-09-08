# -*- coding: utf-8 -*-
import itertools
from collections import defaultdict
from .errors import *
from .dev import is_experimental
from .op import Signature
from .common import get_logger

__all__ = (
            "Session",
        )

"""List of modules with operations to be loaded for the default contex."""

# FIXME: make this more intelligent and lazy
_default_op_modules = (
            "bubbles.backends.sql.ops",
            "bubbles.backends.mongo.ops",
            "bubbles.ops.rows",
            "bubbles.ops.generic",
        )


class Session(object):
    # TODO: add parent_context
    def __init__(self, retry_allow=None, retry_deny=None):
        """ Create a session for pipeline execution. The session holds open
        stores.
        """

        self.logger = get_logger()

        self.retry_allow = retry_allow or []
        self.retry_deny = retry_deny or []


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


# XXX XXX OBSOLETE CODE BELOW XXX XXX

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

