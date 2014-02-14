# -*- Encoding: utf8 -*-

from .errors import *
from .metadata import *
from .extensions import Extensible, extensions
from .objects import data_object
import os.path

__all__ = [
        "DataStore",
        "SimpleMemoryStore",
        "open_store",
        "copy_object"
        ]


def open_store(type_, *args, **kwargs):
    """Opens datastore of `type`."""

    store = extensions.store(type_, *args, **kwargs)
    return store


class DataStore(Extensible):
    __extension_type__ = "store"
    __extension_suffix__ = "Store"
    def __init__(self, **options):
        pass

    def close(self):
        pass

    def clone(self, *args, **kwargs):
        """Returns a clone of the store with different options. Objects coming
        from cloned store might be composable with objects in the original
        store."""
        raise NotImplementedError

    def object_names(self):
        """Returns list of all object names contained in the store"""
        raise NotImplementedError

    def objects(self, names=None, autoload=False):
        """Return list of objects, if available

        * `names`: only objects with given names are returned
        * `autoload`: load object list if necessary, otherwise cached version
          is used if store cachces object metadata.

        Note that loading list of objects might be costly operation in some
        cases.
        """
        raise NotImplementedError

    def get_object(self, name, **args):
        """Subclasses should implement this"""
        raise NotImplementedError

    def __getitem__(self, name):
        return self.get_object(name)

    def create(self, name, fields, replace=False, from_obj=None, temporary=False,
               **options):
        """Args:
            * replace
            * form_obj: object from which the target is created
            * temporary: table is destroyed after store is closed or
              disconnected
        """
        pass

    def exists(self, name):
        """Return `True` if object with `name` exists, otherwise returns
        `False`. Subclasses should implement this method."""
        raise NotImplementedError

    def create_temporary(fields, from_obj=None, **options):
        """Creates a temporary data object"""
        raise NotImplementedError

    def truncate(self, name, *args, **kwargs):
        obj = self.get_object(name, *args, **kwargs)
        obj.truncate()

    def rename(name, new_name, force=False):
        """Renames object from `name` to `new_name`. If `force` is ``True``
        then target is lost"""
        raise NotImplementedError


class SimpleMemoryStore(DataStore):
    def __init__(self):
        """Creates simple in-memory data object store. Useful for temporarily
        store objects. Creates list based objects with `rows` and `records`
        representations."""

        super(SimpleMemoryStore, self).__init__()
        catalogue = {}

    def objects(self):
        return list(catalogue.keys())

    def get_object(self, name):
        try:
            return catalogue[name]
        except KeyError:
            raise NoSuchObjectError(name)

    def create(name, fields, replace=False, from_obj=None, temporary=False,
               **options):
        """Creates and returns a data object that wraps a Python list as a
        data container."""

        if not replace and self.exists(name):
            raise ObjectExistsError(name)

        obj = RowListDataObject(fields)
        catalogue[name] = obj
        return obj

    def exists(name):
        return name in catalogue


class FileSystemStore(DataStore):
    __identifier__ = "file"

    def __init__(self, path):
        """Creates a store for source objects stored on a local file system.
        The type of the object is determined from the file extension.
        Supported extensions and file types:

        * `csv` - CSV source object (read-only)
        * `xls` – MS Excel object
        """

        super().__init__()
        self.path = path

    def get_object(self, name):
        """Returns a CSVSource object with filename constructed from store's
        path and extension"""
        path = os.path.join(self.path, name)
        ext = os.path.splitext(name)[1]

        ext = ext[1:] if ext else ext

        if ext == "csv":
            return data_object("csv_source", path)
        elif ext == "xls":
            return data_object("xls", path)
        else:
            raise ArgumentError("Unknown extension '%s'" % ext)


def copy_object(source_store, source_name, target_store,
                target_name=None, create=False, replace=False):
    """Convenience method that copies object data from source store to target
    store. `source_object` and `target_object` should be object names within
    the respective stores. If `target_name` is not specified, then
    `source_name` is used."""

    target_name = target_name or source_name

    source = source_store.get_object(source_name)
    if create:
        if not replace and target_store.exists(target_name):
            raise Exception("Target object already exists. Use reaplce=True to "
                            "delete the object object and create replacement")
        target = target_store.create(target_name, source.fields, replace=True,
                                     from_obj=source)
    else:
        target = target_store.get_object(target_name)
        target.append_from(source)
        target.flush()

    return target

