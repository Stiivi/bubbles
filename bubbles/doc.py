# -*- Encoding: utf8 -*-

"""bubbles documentation utilities - functions used for documenting bubbles
functionalities and objcects."""

def experimental(fn):
    """Mark a pethod as experimental. Interface or implementation of an
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

def required(fn):
    """Mark method as required to be implemented by scubclasses"""
    fn._bubbles_required = True
    return fn

def recommended(fn):
    """Mark method as recommended to be implemented by subclasses"""
    fn._bubbles_recommended = True
    return fn

