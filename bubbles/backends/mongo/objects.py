
# -*- coding: utf-8 -*-
from ...objects import *
from ...errors import *
from ...common import get_logger
from ...metadata import Field, FieldList
from ...stores import DataStore
from ...datautil import collapse_record

__all__ = (
        "MongoDBStore",
        "MongoDBCollection",
    )

try:
    import pymongo
except:
    from ...common import MissingPackage
    pymongo = MissingPackage("pymongo", "MongoDB streams", "http://www.mongodb.org/downloads/")

"""List of default shared stores."""
_default_stores = {}

def default_store(database, host, port):
    """Gets a default store for connectable or URL. If store does not exist
    one is created and added to shared default store pool."""

    key = (database, host, port)
    try:
        store = _default_stores[key]
    except KeyError:
        store = MongoDBStore(database, host=host, port=port)
        _default_stores[key] = store

    return store

class MongoDBStore(DataStore):
    _ns_object_name = "mongo"

    def __init__(self, database, host='localhost', port=27017, client=None):
        """Creates a MongoDB data object store."""

        if client and (host or port):
            raise ArgumentError("Either client or host/port should be "
                                "specified, not both.")

        if client:
            self.client = client
        else:
            self.client = pymongo.MongoClient(host=host, port=port)

        self.database = self.client[database]

        self.host = host
        self.port = port

    def objects(self, names=None):
        raise NotImplementedError

    def create(self, name, fields, replace=False):
        obj = MongoDBCollection(name, fields=fields, store=self)

        if replace:
            # The only way how to check for collection existence is to look if
            # there are any objects in it
            if len(obj) > 0:
                raise ObjectExistsError("Collection %s already exists" % name)
            else:
                obj.truncate()

        return obj

    def get_object(self, name):
        obj = MongoDBCollection(name, store=self)


class MongoDBCollection(DataObject):
    """docstring for ClassName
    """
    _ns_object_name = "mongo"

    def __init__(self, collection, fields, truncate=False,
                 expand=False,
                 database=None, host='localhsot', port=27017,
                 store=None):
        """Creates a MongoDB data object.

        Attributes
        * `collection`: mongo collection name
        * `database`: database name
        * `host`: mongo database server host, default is ``localhost``
        * `port`: mongo port, default is ``27017``
        * `expand`: expand dictionary values and treat children as top-level
          keys with dot '.' separated key path to the child.
        * `store`: MongoDBStore owning the object

        Specify either store or database, not both.
        """
        super(MongoDBCollection, self).__init__()

        if store and database:
            raise ArgumentError("Both store and database spectified")

        if store:
            self.store = store
        else:
            self.store = _default_store(database, host, port)

        if isinstance(collection, str):
            self.collection = self.store.database[collection]
        else:
            self.collection = collection

        if expand is None:
            self.expand = store.expand
        else:
            self.expand = expand

        if not fields:
            raise NotImplementedError("MongoDB field detection is not yet "
                                      "implemented, please specify them "
                                      "manually")
        self.fields = fields

        if truncate:
            self.truncate()

    def clone(self, fields=None, expand=None):
        fields = fields or self.fields
        if expand is None:
            expand = self.expand

        return MongoDBCollection(collection=self.collection, store=self.store,
                             fields=fields, expand=expand)

    def representations(self):
        return ["mongo", "records", "rows"]

    def is_consumable(self):
        return False

    def truncate(self):
        self.collection.remove()

    def __len__(self):
        return self.collection.count()

    def rows(self):
        fields = self.fields.names()
        iterator = self.collection.find(fields=fields)
        return MongoDBRowIterator(iterator, fields, self.expand)

    def records(self):
        fields = self.field.names()
        iterator = self.collection.find(fields=fields)
        return MongoDBRecordIterator(iterator, self.expand)

    def append(self, obj):
        if type(obj) == dict:
            record = obj
        else:
            record = dict(zip(self.fields.names(), obj))

        if self.expand:
            record = expand_record(record)

        self.collection.insert(record)

class MongoDBRowIterator(object):
    """Wrapper for pymongo.cursor.Cursor to be able to return rows() as tuples and records() as
    dictionaries"""
    def __init__(self, cursor, field_names, expand):
        self.cursor = cursor
        self.field_names = field_names
        self.expand = expand

    def __iter__(self):
        return self

    def __next__(self):
        record = next(self.cursor)

        if not record:
            raise StopIteration

        if self.expand:
            row = []
            for field in self.field_names:
                value = record
                for key in field.split('.'):
                    if key in value:
                        value = value[key]
                    else:
                        break
                array.append(value)
        else:
            row = [record[field] for field in self.field_names]

        return row

class MongoDBRecordIterator(object):
    """Wrapper for pymongo.cursor.Cursor to be able to return rows() as tuples and records() as
    dictionaries"""
    def __init__(self, cursor, expand=False):
        self.cursor = cursor
        self.expand = expand

    def __iter__(self):
        return self

    def __next__(self):
        record = next(cursor)

        if not record:
            raise StopIteration

        if not self.expand:
            return record
        else:
            return collapse_record(record)
