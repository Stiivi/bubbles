# -*- coding: utf-8 -*-

from contextlib import ContextDecorator
from .errors import ArgumentError

import urllib.request
import urllib.parse
import codecs
import json

__all__ = (
    "Resource",
    "is_local",
    "read_json",
)


class Resource(ContextDecorator):
    def __init__(self, url=None, handle=None, opener=None, encoding=None,
                 binary=False):
        """Creates a data resource for reading. Arguments:

        * `url` – resource URL or a local path
        * `handle` – handle of an opened file. If provided, then the resource
          `url` is just informative (for display or debugging purposes) and
          should correspond to the opened handle.
        * `opener` – a function that opens the URL
        * `encoding` – encoding used on local files or URLs with default URL
          opener
        * `binary` – `True` if the resource is binary, `False` (default) if it
          is a text

        The resource can be used as a context manager: `with Resource(url) as
        f: ...`.

        """

        if url is None and handle is None:
            raise ArgumentError("Either resource url or handle should be "
                                "provided. None was given.")

        self.url = url
        self.binary = binary
        self.encoding = encoding

        self.reader = None

        if not opener:
            if is_local(url):
                self.opener = None
            else:
                self.opener = urllib.request.urlopen
                if self.encoding:
                    self.reader = codecs.getreader(self.encoding)
        else:
            self.opener = opener

        self.handle = handle

    # should_close can't go in the constructor, since later calling open() may
    # implicitly change its value
    @property
    def should_close(self):
        return self.handle is not None

    def open(self):
        if self.handle:
            return self.handle

        if self.opener:
            self.handle = self.opener(self.url)
        else:
            mode = "rb" if self.binary else "r"
            self.handle = open(self.url, mode=mode, encoding=self.encoding)
        if self.reader:
            self.handle = self.reader(self.handle)

        return self.handle

    def close(self):
        if self.should_close:
            self.handle.close()

    def __enter__(self):
        return self.open()

    def __exit__(self, *exc):
        self.close()


def is_local(url):
    """Returns path to a local file from `url`. Returns `None` if the `url`
    does not represent a local file."""
    parts = urllib.parse.urlparse(url)
    return parts.scheme == '' or parts.scheme == 'file'

def open_resource(resource, mode=None, encoding=None, binary=False):
    raise NotImplementedError("open_resource is deprecated, use Resource")

def read_json(url):
    """Reads JSON from `url`. The `url` can also be a local file path."""
    with Resource(url) as f:
        try:
            data = json.load(f)
        except Exception as e:
            raise Exception("Unable to read JSON from %s: %s"
                            % (url, str(e)))
    return data
