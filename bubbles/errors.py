# -*- coding: utf-8 -*-
"""Bubbles Exceptions"""

class BubblesError(Exception):
	"""Basic error class"""
	pass

class UserError(BubblesError):
    """Superclass for all errors caused by the bubbles users. Error
    messages from this error might be safely passed to the front-end. Do not
    include any information that you would not like to be public"""
    error_type = "unknown_user_error"

class InternalError(BubblesError):
    """Superclass for all errors that happened internally: configuration
    issues, connection problems, model inconsistencies..."""

class PipelineError(UserError):
    """Raised when pipeline is misconfigured"""
    pass

class ConfigurationError(InternalError):
    """Raised when there is a problem with workspace configuration assumed."""

class MetadataError(BubblesError):
	"""Error raised on metadata inconsistency"""
	pass

class NoSuchFieldError(MetadataError):
	"""Error raised on metadata inconsistency"""
	pass

class FieldOriginError(MetadataError):
    """Error with field origin, such as circular reference."""
    pass

class ArgumentError(BubblesError):
    """Raised when wrong argument is passed to a function"""
    pass

class ProbeAssertionError(BubblesError):
    """Raised when probe assertion fails"""
    def __init__(self, reason=None):
        self.reason = reason

class ConsumedError(BubblesError):
    """Raised when trying to read from already consumed object"""
    pass
#
# DataObject and DataStore errors
#

class DataObjectError(BubblesError):
    """Generic error in a data object."""

class NoSuchObjectError(DataObjectError):
    """Raised when object does not exist."""
    pass

class ObjectExistsError(DataObjectError):
    """Raised when attempting to create and object that already exists"""
    pass

class IsNotTargetError(DataObjectError):
    """Raised when trying to use data object as target - appending data,
    truncating or any other target-only operation."""
    pass

class IsNotSourceError(DataObjectError):
    """Raised when trying to use data object as source, for example reading
    data"""
    pass

class RepresentationError(DataObjectError):
    """Raised when requested unknown, invalid or not available
    representation"""
    pass

#
# Operations errors
#

class OperationError(BubblesError):
    """Raised when operation is not found or is mismatched"""
    pass

class RetryOperation(Exception):
    """Raised within operation to signal to the kernel that it should try
    another operation with different signature."""
    def __init__(self, signature=None, reason=None):
        self.signature = signature
        self.reason = reason

class RetryError(BubblesError):
    """Raised when operation was retried too many times"""
    pass

class GraphError(BubblesError):
    pass

class FieldError(BubblesError):
    """Raised when wrong field types are passed to an operation."""
    pass
