#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import io
import os.path
from collections import defaultdict, namedtuple
import itertools
from ...objects import *
from ...metadata import *
from ...errors import *
from ...resource import Resource
from ...stores import DataStore
import json
from datetime import datetime
from time import strptime
from base64 import b64decode
import json

__all__ = (
        "CSVStore",
        "CSVSource",
        "CSVTarget",
        )


CSVData = namedtuple("CSVData", ["handle", "dialect", "encoding", "fields"])

# TODO: add type converters
# TODO: handle empty strings as NULLs

class CSVStore(DataStore):
    def __init__(self, path, extension=".csv", role=None, **kwargs):
        super(CSVStore, self).__init__()
        self.path = path
        self.extension = extension
        if role:
            self.role = role.lower
        else:
            self.role = "source"

        self.kwargs = kwargs

    def get_object(self, name):
        """Returns a CSVSource object with filename constructed from store's
        path and extension"""
        fields_file = "%s_fields.json" % name
        fields_path = os.path.join(self.path, fields_file)

        name = name + self.extension
        path = os.path.join(self.path, name)

        args = dict(self.kwargs)
        if os.path.exists(fields_path):
            with open(fields_path) as f:
                metadata = json.load(f)
            args["fields"] = FieldList(*metadata)

        if self.role in ["s", "src", "source"]:
            return CSVSource(path, **args)
        elif self.role in ["t", "target"]:
            return CSVTarget(path, **args)
        else:
            raise ArgumentError("Unknown CSV object role '%s'" % role)

    def create(self, name, fields, replace=False):
        """Create a file"""
        name = name + self.extension
        path = os.path.join(self.path, name)

        target = CSVTarget(path, fields=fields, truncate=True)
        return target

class CSVSource(DataObject):
    """Comma separated values text file as a data source."""

    _bubbles_info = {
        "attributes": [
            {
                "name":"resource",
                "description": "file name, URL or a file handle with CVS data"
            },
            {
                "name":"fields",
                "description": "fields in the file. Should be set if read_header "
                               "is false"
            },
            {
                "name":"fields",
                "description": "flag determining whether first line contains "
                                "header or not. ``True`` by default."
            },
            {
                "name":"encoding",
                "description":"file encoding"
            },
            {
                "name":"read_header",
                "description":"flag whether file header is read or not"
            },
            {
                "name":"skip_rows",
                "description":"number of rows to be skipped"
            },
            {
                "name": "empty_as_null",
                "description": "Treat emtpy strings as NULL values"
            },
            {
                "name": "type_converters",
                "description": "dictionary of data type converters"
            }
        ]
    }

    def __init__(self, resource, read_header=True, dialect=None,
            delimiter=None, encoding=None, skip_rows=None,
            empty_as_null=True, fields=None, type_converters=None, **options):
        """Creates a CSV data source stream.

        * `resource`: file name, URL or a file handle with CVS data
        * `read_header`: flag determining whether first line contains header
          or not. ``True`` by default.
        * `encoding`: source character encoding, by default no conversion is
          performed.
        * `fields`: optional `FieldList` object. If not specified then
          `read_header` should be used.
        * `skip_rows`: number of rows to be skipped. Default: ``None``
        * `empty_as_null`: treat empty strings as ``Null`` values
        * `type_converters`: dictionary of converters (functions). It has
          to cover all knowd types.

        Note: avoid auto-detection when you are reading from remote URL
        stream.

        Rules for fields:

        * if `fields` are specified, then they are used, header is ignored
          depending on `read_header` flag
        * if `detect_types` is not requested, then each field is of type
          `string` (this is the default)
        """

        self.file = None

        if not any((fields, read_header)):
            raise ArgumentError("At least one of fields or read_header"
                                " should be specified")

        self.read_header = read_header
        self.encoding = encoding
        self.empty_as_null = empty_as_null

        self.dialect = dialect
        self.delimiter = delimiter

        self.skip_rows = skip_rows or 0
        self.fields = fields
        # TODO: use default type converters
        self.type_converters = type_converters or {}

        self.resource = Resource(resource, encoding=self.encoding)
        self.handle = self.resource.open()

        options = dict(options) if options else {}
        if self.dialect:
            if isinstance(self.dialect, str):
                options["dialect"] = csv.get_dialect(self.dialect)
            else:
                options["dialect"] = self.dialect
        if self.delimiter:
            options["delimiter"] = self.delimiter

        # CSV reader options
        self.options = options

        # self.reader = csv.reader(handle, **self.reader_args)
        self.reader = csv.reader(self.handle, **options)


        if self.skip_rows:
            for i in range(0, self.skip_rows):
                next(self.reader)

        # Initialize field list
        if self.read_header:
            field_names = next(self.reader)

            # Fields set explicitly take priority over what is read from the
            # header. (Issue #17 might be somehow related)
            if not self.fields:
                fields = [ (name, "string", "default") for name in field_names]
                self.fields = FieldList(*fields)

        if not self.fields:
            raise RuntimeError("Fields are not initialized. "
                               "Either read fields from CSV header or "
                               "set them manually")

        self.set_fields(self.fields)


    def set_fields(self, fields):
        self.converters = [self.type_converters.get(f.storage_type) for f in fields]

        if not any(self.converters):
            self.converters = None

    def release(self):
        if self.resource:
            self.resource.close()

    def representations(self):
        return ["csv", "rows", "records"]

    def rows(self):
        missing_values = [f.missing_value for f in self.fields]

        for row in self.reader:
            result = []

            for i, value in enumerate(row):
                if self.empty_as_null and not value:
                    result.append(None)
                    continue

                if missing_values[i] and value == missing_values[i]:
                    result.append(None)
                    continue

                func = self.converters[i] if self.converters else None

                if func:
                    result.append(func(value))
                else:
                    result.append(value)
            yield result

    def csv_stream(self):
        return self.handle

    def records(self):
        fields = self.fields.names()
        for row in self.reader:
            yield dict(zip(fields, row))

    def is_consumable(self):
        return True

    def retained(self):
        """Returns retained copy of the consumable"""
        # Default implementation is naive: consumes whole CSV into Python
        # memory
        # TODO: decide whether source is seek-able or not

        return RowListDataObject(list(self.rows()), self.fields)


class CSVTarget(DataObject):
    """Comma separated values text file as a data target."""

    _bubbles_info = {
        "attributes": [
            {
                "name":"resource",
                "description": "Filename or URL"
            },
            {
                "name": "write_headers",
                "description": "Flag whether first row will contain field names"
            },
            {
                "name": "truncate",
                "description": "If `True` (default) then target file is truncated"
            },
            {
                "name": "encoding",
                "description": "file character encoding"
            },
            {
                "name": "fields",
                "description": "data fields"
            }
        ]
    }

    def __init__(self, resource, write_headers=True, truncate=True,
                 encoding="utf-8", dialect=None,fields=None, **kwds):
        """Creates a CSV data target

        :Attributes:
            * resource: target object - might be a filename or file-like
              object
            * write_headers: write field names as headers into output file
            * truncate: remove data from file before writing, default: True

        """
        self.write_headers = write_headers
        self.truncate = truncate
        self.encoding = encoding
        self.dialect = dialect
        self.fields = fields
        self.kwds = kwds

        self.close_file = False
        self.handle = None

        mode = "w" if self.truncate else "a"

        self.handle = open(resource, mode=mode, encoding=encoding)

        self.writer = csv.writer(self.handle, dialect=self.dialect, **self.kwds)

        if self.write_headers:
            if not self.fields:
                raise BubblesError("No fields provided")
            self.writer.writerow(self.fields.names())

        self.field_names = self.fields.names()

    def finalize(self):
        if self.handle:
            self.handle.close()

    def append(self, row):
        self.writer.writerow(row)

    def append_from(self, obj):
        for row in obj:
            self.append(row)


