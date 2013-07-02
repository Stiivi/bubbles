# -*- Encoding: utf8 -*-

"""Data Objects"""

# FIXME: add this
# from .ops.iterator import as_records
from .errors import *
from .extensions import *
from .metadata import *
from .doc import required, experimental

__all__ = [
        "DataObject",
        "IterableDataSource",
        "RowListDataObject",
        "IterableRecordsDataSource",

        "shared_representations",
        "data_object",
        ]

def data_object(type_, *args, **kwargs):
    """Returns a data object of specified `type_`. Arguments are passed to
    respective data object factory.

    Available data object type in the base framework:

    * `sql_table`
    * `sql`
    * `csv_source`
    * `csv_target`
    * `iterable`
    * `iterable_records`
    * `row_list`
    """

    ns = get_namespace("object_types")
    if not ns:
        ns = initialize_namespace("object_types", root_class=DataObject,
                                    suffix=None)

    try:
        factory = ns[type_]
    except KeyError:
        raise BubblesError("Unable to find factory for object of type %s" %
                                type_)
    return factory(*args, **kwargs)

def iterator_object(iterator, fields):
    """Returns a data object wrapping an `iterator` with rows of `fields`."""
    return IterableDataSource(iterator, fields)

class DataObject(object):
    def representations(self):
        """Returns list of representation names of this data object. Default
        implementation raises an exception, as subclasses are required to
        implement this method.

        Representations do not have to be known up front – they might depend
        on various run-time conditions. Refer to particular object
        documentation.
        """
        raise NotImplementedError("Subclasses are required to specify "
                                  "representations of a data object.")

    def is_compatible(self, obj, required=None, ignored=None):
        """Returns `True` when the receiver and the `object` share
        representations. `required` contains list of representations that at
        least one of them is required. If not present, then returns `False`.
        `ignored` is a list of representations that are not relevant for the
        comparison."""
        required = set(required or [])
        ignored = set(ignored or [])
        ours = set(self.representations() or [])
        theirs = set(obj.representations or [])

        reprs = (ours - ignored) & (theirs - ignored)
        if required:
            reprs = reprs & required

        return len(reprs) > 0

    @required
    def can_compose(self, obj):
        """Returns `True` when any of the representations can be naturally
        (without a proxy) composed by any of the representations of `obj` to
        form new representation.  Example of composable objects are SQL
        statement object from the same engine. Subclasses should implement
        this method. Default implementation returns `False`, which means that
        the only suggested composition is to use iterators.

        The method should be transient. That is, if A can be naturally
        composed with B and B with C, then A can be naturally composed with C
        as well. This property is for simplification of finding whether a list
        of objects can be composed together or not.
        """

        return False

    def truncate(self):
        """Removes all records from the target table"""
        raise NotImplementedError

    def flush(self):
        """Flushes oustanding data. Default implementation does nothing.
        Subclasses might implement this method if necessary."""
        pass

    def is_consumable(self):
        """Returns `True` if the object is consumed when used, returns `False`
        when object can be used indefinitely number of times."""
        raise NotImplementedError("Data objects are required to implement "
                                  "is_consumable() method")

    def retained(self, count=1):
        """Returns object's replacement which can be consumed `count` times.
        Implementation of object retention depends on the backend.

        For example default iterable data object consumes the rows into a list
        and provides data object which wraps the list and deletes the list
        after `count` number of uses.

        .. note::

            If the object's retention policy is not appropriate for your task
            (memory or time hungry), it is recommended to cache the object
            content before consumption.

        .. note::

            Not all objects or all representations might be retained. Refer
            to particular object implementation for more information.

        Default implementation returns the receiver if it is not consumable
        and raises an exception if it is consumable. Consumable objects should
        implement this method.
        """
        if self.is_consumable():
            return self
        else:
            raise NotImplementedError

            # TODO: should we use this? Isn't it too dangerous to use this
            # very naive and hungry implementation? It is definitely
            # convenient.
            #
            # return RowListDataSource(self.rows(), self.fields)

    def __iter__(self):
        return self.rows()

    def records(self):
        """Returns an iterator of records - dictionary-like objects that can
        be acessed by field names. Default implementation returns
        dictionaries, however other objects mith return their own structures.
        For example the SQL object returns the same iterator as for rows, as
        it can serve as key-value structure as well."""

        names = [str(field) for field in fields]

        for row in self.rows():
            yield dict(zip(names, row))

    def append(self, row):
        """Appends `row` to the object. Subclasses should implement this."""
        raise NotImplementedError

    def append_from(self, obj):
        """Appends data from object `obj` which might be a `DataObject`
        instance or an iterable. Default implementation uses iterable and
        calls `append()` for each element of the iterable.

        This method executes the append instead of returning a composed
        object. For object composition see `append` operation.
        """
        for row in iter(obj):
            self.append(row)

    def append_from_iterable(self, iterator):
        """Appends data from iterator which has same fields as the receiver.
        This method actualy executes the append instead of returning a
        composed object. For object composition see `append` operation"""
        obj = IterableDataSource(iterator, self.fields)
        self.append_from(obj)

    def as_source(self):
        """Returns version of the object that can be used as source. Subclasses
        might return an object that will raise exception on attempt to use
        target-only methods such as appending.

        Default implementation returns the receiver."""
        return self

    def as_target(self):
        """Returns version of the object that can be used as target. Subclasses
        might return an object that will raise exception on attempt to use
        source-only methods such as iterating rows.

        Default implementation returns the receiver."""
        return self


def shared_representations(objects):
    """Returns representations that are shared by all `objects`"""
    objects = list(objects.values())
    reps = set(objects[0].representations())
    for obj in objects[1:]:
        reps &= set(obj.representations())

    return reps

class IterableDataSource(DataObject):
    """Wrapped Python iterator that serves as data source. The iterator should
    yield "rows" – list of values according to `fields` """

    _ns_object_name = "iterable"

    _bubbles_info = {
        "attributes": [
            {"name":"iterable", "description": "Python iterable object"},
            {"name":"fields", "description":"fields of the iterable"}
        ]
    }


    def __init__(self, iterable, fields):
        """Create a data object that wraps an iterable."""
        self.fields = fields
        self.iterable = iterable

    def representations(self):
        """Returns the only representation of iterable object, which is
        `rows`"""
        return ["rows", "records"]

    def rows(self):
        return iter(self.iterable)

    def records(self):
        return as_records(self.iterable, self.fields)

    def is_consumable(self):
        return True

    def retained(self, retain_count=1):
        """Returns retained replacement of the receiver. Default
        implementation consumes the iterator into a list and returns a data
        object wrapping the list, which might leave big memory footprint on
        larger datasets. In this case it is recommended to explicitly cache
        the consumable object using other means.
        """

        return RowListDataSource(list(self.iterable), self.fields)

    def filter(self, keep=None, drop=None, rename=None):
        """Returns another iterable data source with filtered fields"""

        ffilter = FieldFilter(keep=keep, drop=drop, rename=rename)
        fields = ffilter.filter(self.fields)

        if keep or drop:
            iterator = ops.iterator.field_filter(self.iterable, self.fields,
                                                 ffilter)
        else:
            # No need to filter if we are just renaming, reuse the iterator
            iterator = self.iterable

        return IterableDataSource(iterator, fields)


class IterableRecordsDataSource(IterableDataSource):
    """Wrapped Python iterator that serves as data source. The iterator should
    yield "records" – dictionaries with keys as specified in `fields` """

    _ns_object_name = "iterable_records"

    _bubbles_info = {
        "attributes": [
            {"name":"iterable", "description": "Python iterable object"},
            {"name":"fields", "description":"fields of the iterable"}
        ]
    }

    def rows(self):
        names = [str(field) for field in self.fields]
        for record in self.iterable:
            yield [record[f] for f in names]

    def records(self):
        return iter(self.iterable)

    def is_consumable(self):
        return True


class RowListDataObject(DataObject):
    """Wrapped Python list that serves as data source or data target. The list
    content are "rows" – lists of values corresponding to `fields`.

    If list is not provided, one will be created.
    """

    _ns_object_name = "list"

    _bubbles_info = {
        "attributes": [
            {"name":"data", "description": "List object."},
            {"name":"fields", "description":"fields of the iterable"}
        ]
    }

    def __init__(self, data=None, fields=None):
        """Create a data object that wraps an iterable. The object is dumb,
        does not perform any field checking, accepts anything passed to it.
        `data` should be appendable list-like object, if not provided, empty
        list is created."""

        self.fields = fields
        if data is None:
            self.data = []
        else:
            self.data = data

    def representations(self):
        """Returns the only representation of iterable object, which is
        `rows`"""
        return ["rows", "records"]

    def rows(self):
        return iter(self.data)

    def records(self):
        return as_records(self.rows(), self.fields)

    def is_consumable(self):
        return False

    def append(self, row):
        self.data.append(row)

    def truncate(self):
        self.data = []


class RowToRecordConverter(object):
    def __init__(self, fields, ignore_empty=False):
        """Creates a converter from rows (list) to a record (dictionary). If
        `ignore_empty` is `True` then keys for empty field values are not
        created."""

        self.field_names = (str(field) for field in fields)
    def __call__(self, row):
        """Returns a record from `row`"""
        if ignore_empty:
            items = ((k, v) for (k, v) in zip(field_names, row) if v is not None)
            return dict(items)
        else:
            return dict(list(zip(field_names, row)))

class RecordToRowConverter(object):
    def __init__(self, fields):
        """Creates a converter from records (dict) to rows (list)."""
        self.field_names = (str(field) for field in fields)

    def __call__(self, record):
        row = (record.get(name) for name in self.field_names)
        return row

