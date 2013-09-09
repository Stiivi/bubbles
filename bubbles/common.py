# -*- coding: utf-8 -*-
# Common bubbles functions and classes - related to data processing or process
# management
#
# For language utility functions see module util

import re
import sys
import logging

__all__ = [
    "logger_name",
    "get_logger",
    "create_logger",

    "MissingPackage",

    "decamelize",
    "to_identifier",
]

logger_name = "bubbles"
logger = None

def get_logger():
    """Get bubbles default logger"""
    global logger

    if logger:
        return logger
    else:
        return create_logger()

def create_logger():
    """Create a default logger"""
    global logger
    logger = logging.getLogger(logger_name)

    formatter = logging.Formatter(fmt='%(asctime)s %(levelname)s %(message)s')

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger

class MissingPackageError(Exception):
    """Exception raised when encountered a missing package."""
    pass

class MissingPackage(object):
    """Bogus class to handle missing optional packages - packages that are not
    necessarily required for Cubes, but are needed for certain features."""

    def __init__(self, package, feature = None, source = None, comment = None):
        self.package = package
        self.feature = feature
        self.source = source
        self.comment = comment

    def __call__(self, *args, **kwargs):
        self._fail()

    def __getattr__(self, name):
        self._fail()

    def _fail(self):
        if self.feature:
            use = " to be able to use: %s" % self.feature
        else:
            use = ""

        if self.source:
            source = " from %s" % self.source
        else:
            source = ""

        if self.comment:
            comment = ". %s" % self.comment
        else:
            comment = ""

        raise MissingPackageError("Optional package '%s' is not installed. "
                                  "Please install the package%s%s%s" %
                                      (self.package, source, use, comment))

def decamelize(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1 \2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1 \2', s1)

def to_identifier(name):
    return re.sub(r' ', r'_', name).lower()



