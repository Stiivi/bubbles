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
from ...urlresource import open_resource
from ...stores import DataStore
from ...datautil import guess_type

__all__ = (
        "CSVStore",
        "CSVSource",
        "CSVTarget",
        )

def to_bool(value):
    """Return boolean value. Convert string to True when "true", "yes" or "on"
    """
    if isinstance(value, str):
        value = value.lower()
        return value in ["1", "true", "yes", "on"] and value != "0"
    else:
        return bool(value)

_default_type_converters = {
    "unknown": None,
    "string": None,
    "text": None,
    "integer": int,
    "float": float,
    "boolean": to_bool,
    "date": None,
    "time": None
}

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
        name = name + self.extension
        path = os.path.join(self.path, name)

        if self.role in ["s", "src", "source"]:
            return CSVSource(path, **self.kwargs)
        elif self.role in ["t", "target"]:
            return CSVTarget(path, **self.kwargs)
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
                               "or infer_fields is false"
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
                "name":"infer_fields",
                "description":"Try to determine number and data type of fields "
                              "This option requires the resource to be seek-able. "
                              "Very likely does not work on remote streams."
            },
            {
                "name":"sample_size",
                "description":"Number of rows to read for type detection."
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
            delimiter=None, encoding=None, sample_size=1024, skip_rows=None,
            empty_as_null=True, fields=None, infer_fields=False,
            type_converters=None, **options):
        """Creates a CSV data source stream.

        :Attributes:
            * `resource`: file name, URL or a file handle with CVS data
            * `read_header`: flag determining whether first line contains header
              or not. ``True`` by default.
            * `encoding`: source character encoding, by default no conversion is
              performed.
            * `fields`: optional `FieldList` object. If not specified then
              `read_header` and/or `infer_fields` should be used.
            * `infer_fields`: try to determine number and data type of fields.
              This option requires the resource to be seek-able, like files.
              Does not work on remote streams.
            * `sample_size`: number of rows to read for type detection if
              `detect_types` is ``True``. 0 means all rows.
              and headers in file. By default it is set to 200 bytes to
              prevent loading huge CSV files at once.
            * `skip_rows`: number of rows to be skipped. Default: ``None``
            * `empty_as_null`: treat empty strings as ``Null`` values
            * `type_converters`: dictionary of converters (functions). It has
              to cover all knowd types.

        Note: avoid auto-detection when you are reading from remote URL
        stream.

        Rules for fields:

        * if `fields` are specified, then they are used, header is ignored
          depending on `read_header` flag
        * if `detect_types` is requested, then types are infered from
          `sample_size` number of rows
        * if `detect_types` is not requested, then each field is of type
          `string` (this is the default)
        """

        """
        RH = request header, FI = fields, IT = infer types

        RH FI IT
         0  0  0 - ERROR
         0  0  1 - detect fields
         1  0  0 - read header, use strings
         1  0  1 - read header, detect types
         0  1  0 - use fields, header as data
         0  1  1 - ERROR
         1  1  0 - ignore header, use fields
         1  1  1 - ERROR
        """
        # FIXME: loosen requirement for type_converters to contain all known
        # types

        self.file = None

        if not any((fields, read_header, infer_fields)):
            raise ArgumentError("At least one of fields, read_header or "
                                "infer_fields should be specified")

        if fields and infer_fields:
            raise ArgumentError("Fields provided and field inference "
                                "requested. They are exclusive, use only one")

        self.read_header = read_header
        self.encoding = encoding
        self.empty_as_null = empty_as_null

        self.dialect = dialect
        self.delimiter = delimiter

        self.skip_rows = skip_rows or 0
        self.fields = fields
        self.do_infer_fields = infer_fields
        self.sample_size = sample_size
        self.type_converters = type_converters or _default_type_converters

        # Fetched sample for infering fields
        self._sample = []

        """Initialize CSV source stream:

        #. perform autodetection if required:
            #. detect encoding from a sample data (if requested)
            #. detect whether CSV has headers from a sample data (if
            requested)
        #.  create CSV reader object
        #.  read CSV headers if requested and initialize stream fields

        If fields are explicitly set prior to initialization, and header
        reading is requested, then the header row is just skipped and fields
        that were set before are used. Do not set fields if you want to read
        the header.

        All fields are set to `storage_type` = ``string`` and
        `analytical_type` = ``unknown``.
        """

        self.resource = open_resource(resource, encoding=self.encoding)
        self.handle = self.resource.handle

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

        if self.do_infer_fields:
            self.fields = self.infer_fields()

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

    def infer_fields(self, sample_size=1000):
        """Detects fields from the source. If `read_header` is ``True`` then
        field names are read from the first row of the file. If it is
        ``False`` then field names are `field0`, `field1` ... `fieldN`.

        After detecting field names, field types are detected from sample of
        `sample_size` rows.

        Returns a `FieldList` instance.

        If more than one field type is detected, then the most compatible type
        is returned. However, do not rely on this behavior.

        Note that the source has to be seek-able (like a local file, not as
        remote stream) for detection to work. Stream is reset to its origin
        after calling this method.

        .. note::

            This method is provided for convenience. For production
            environment it is recommended to detect types during development
            and then to use an explicit field list during processing.
        """

        self._sample = []
        for i in range(0, self.skip_rows):
            self._sample.append(next(self.reader))

        if self.read_header:
            row = next(self.reader)
            self._sample.append(row)
            field_names = row
        else:
            field_names = None

        rownum = 0
        probes = defaultdict(set)

        while rownum <= sample_size:
            try:
                row = next(self.reader)
                self._sample.append(row)
            except StopIteration:
                break

            rownum += 1
            for i, value in enumerate(row):
                probes[i].add(guess_type(value))

        keys = list(probes.keys())
        keys.sort()

        types = [probes[key] for key in keys]

        if field_names and len(types) != len(field_names):
            raise Exception("Number of detected fields differs from number"
                            " of fields specified in the header row")
        if not field_names:
            field_names = ["field%d" % i for i in range(len(types))]

        fields = FieldList()

        for name, types in zip(field_names, types):
            if "string" in types:
                t = "string"
            elif "integer"in types:
                t = "integer"
            elif "float" in types:
                t = "float"
            elif "date" in types:
                t = "date"
            else:
                t = "string"
            field = Field(name, t)
            fields.append(field)

        # Prepend already consumed sample before the reader
        self.reader = itertools.chain(iter(self._sample), self.reader)
        return fields

    def set_fields(self, fields):
        try:
            self.converters = [self.type_converters[f.storage_type] for f in fields]
        except KeyError as e:
            raise BubblesError("Unknown conversion: %s" % e)

        if not any(self.converters):
            self.converters = None

    def release(self):
        if self.resource:
            self.resource.close()

    def representations(self):
        return ["csv", "rows", "records"]

    def rows(self):
        for row in self.reader:
            result = []

            for i, value in enumerate(row):
                if self.empty_as_null and not value:
                    result.append(None)
                    continue

                func = self.converters[i] if self.converters else None

                if func:
                    result.append(func(value))
                else:
                    result.append(value)
            yield result

    def csv_data(self):
        s = CSVData(self.handle, self.dialect, self.encoding,
                                                            self.fields)
        return s

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

        self.resource = open_resource(resource, mode,
                                        encoding=self.encoding)
        self.handle = self.resource.handle

        self.writer = csv.writer(self.handle, dialect=self.dialect, **self.kwds)

        if self.write_headers:
            if not self.fields:
                raise BubblesError("No fields provided")
            self.writer.writerow(self.fields.names())

        self.field_names = self.fields.names()

    def finalize(self):
        if self.resource:
            self.resource.close()

    def append(self, row):
        self.writer.writerow(row)

    def append_from(self, obj):
        for row in obj:
            self.append(row)


