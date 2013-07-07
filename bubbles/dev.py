# -*- Encoding: utf8 -*-

"""bubbles development utilities - functions used for development and
documentation of bubbles functionalities and objcects."""

def experimental(fn):
    """Mark a method as experimental. Interface or implementation of an
    experimental method might very likely change. Use with caution. This
    decorator just appends a doc string."""

    warning = \
    """
    .. warning::

       This method is experimental. Interface or implementation might
       change. Use with caution and note that you might have to modify
       your code later.
    """

    if fn.__doc__ is not None:
        fn.__doc__ += warning
    fn._bubbles_experimental = True

    return fn

def is_experimental(fn):
    """Returns `True` if function `fn` is experimental."""

    if hasattr(fn, "_bubbles_experimental"):
        return fn._bubbles_experimental
    else:
        return False

def required(fn):
    """Mark method as required to be implemented by scubclasses"""
    fn._bubbles_required = True
    return fn

def recommended(fn):
    """Mark method as recommended to be implemented by subclasses"""
    fn._bubbles_recommended = True
    return fn

