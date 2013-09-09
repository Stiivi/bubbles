# -*- coding: utf-8 -*-
from ..objects import *
from ..errors import *
from ..common import get_logger
from ..metadata import Field, FieldList
from ..stores import DataStore
from ..urlresource import open_resource
import datetime

try:
    import xlrd
    _cell_types = {
        xlrd.XL_CELL_EMPTY:   'unknown',
        xlrd.XL_CELL_TEXT:    'string',
        xlrd.XL_CELL_NUMBER:  'float',
        xlrd.XL_CELL_DATE:    'date',
        xlrd.XL_CELL_BOOLEAN: 'boolean',
        xlrd.XL_CELL_ERROR:   'unknown',
        xlrd.XL_CELL_BLANK:   'unknown',
    }

except ImportError:
    from ..common import MissingPackage
    xlrd = MissingPackage("xlrd", "Data objects from MS Excel spreadsheets")

def _load_workbook(resource, encoding):
        resource = open_resource(resource, binary=True)
        data = resource.handle.read()
        resource.close()

        workbook = xlrd.open_workbook(file_contents=data,
                                      encoding_override=encoding)

        return workbook

class XLSStore(DataStore):
    def __init__(self, resource, encoding=None):
        super(XLSStore, self).__init__()

        self.resource = resource
        self.encoding = encoding
        self.book = None

    def get_object(self, name, skip_rows=0, has_header=True):
        if not self.book:
            self.book = _load_workbook(self.resource, self.encoding)

        return XLSObject(workbook=self.book, sheet=name, skip_rows=skip_rows,
                            has_header=has_header)

    def object_names(self):
        if not self.book:
            self.book = _load_workbook(self.resource, self.encoding)

        return self.book.sheet_names()

    def create(self, name):
        raise BubblesError("XLS store is read-only")


class XLSObject(DataObject):
    __identifier__ = "xls"
    def __init__(self, resource=None, fields=None, sheet=0, encoding=None,
                 skip_rows=0, has_header=True, workbook=None):
        """Creates a XLS spreadsheet data source stream.

        Attributes:

        * resource: file name, URL or file-like object
        * sheet: sheet index number (as int) or sheet name (as str)
        * read_header: flag determining whether first line contains header or
          not. ``True`` by default.
        """
        if workbook:
            if resource:
                raise ArgumentError("Can not specify both resource and "
                                    "workbook")

            self.workbook = workbook
        else:
            self.workbook = _load_workbook(resource, encoding)

        if isinstance(sheet, int):
            self.sheet = self.workbook.sheet_by_index(sheet)
        else:
            self.sheet = self.workbook.sheet_by_name(sheet)

        if skip_rows >= self.sheet.nrows:
            raise ArgumentError("First row number is larger than number of rows")

        if has_header:
            self.first_row = skip_rows + 1
        else:
            self.first_row  = skip_rows

        if fields:
            self.fields = fields
        else:
            # Read fields
            if has_header:
                names = self.sheet.row_values(skip_rows)

            self.fields = FieldList()

            row = self.sheet.row(self.first_row)
            for name, cell in zip(names, row):
                storage_type = _cell_types.get(cell.ctype, "unknown")
                field = Field(name, storage_type=storage_type)
                self.fields.append(field)


    def representations(self):
        return ["rows", "records"]

    def __len__(self):
        return self.sheet.nrows - self.first_row

    def rows(self):
        if not self.fields:
            raise RuntimeError("Fields are not initialized")
        return XLSRowIterator(self.workbook, self.sheet, self.first_row)

    def records(self):
        fields = self.fields.names()
        for row in self.rows():
            yield dict(zip(fields, row))

    def is_consumable(self):
        return False

class XLSRowIterator(object):
    """
    Iterator that reads XLS spreadsheet
    """
    def __init__(self, workbook, sheet, first_row=0):
        self.workbook = workbook
        self.sheet = sheet
        self.row_count = sheet.nrows
        self.current_row = first_row

    def __iter__(self):
        return self

    def __next__(self):
        if self.current_row >= self.row_count:
            raise StopIteration

        row = self.sheet.row(self.current_row)
        row = tuple(self._cell_value(cell) for cell in row)
        self.current_row += 1
        return row

    def _cell_value(self, cell):
        if cell.ctype == xlrd.XL_CELL_NUMBER:
            return float(cell.value)
        elif cell.ctype == xlrd.XL_CELL_DATE:
            # TODO: distinguish date and datetime
            args = xlrd.xldate_as_tuple(cell.value, self.workbook.datemode)
            return datetime.date(args[0], args[1], args[2])
        elif cell.ctype == xlrd.XL_CELL_BOOLEAN:
            return bool(cell.value)
        else:
            return cell.value

