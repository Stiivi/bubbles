# -*- coding: utf-8 -*-
import copy
import itertools
import functools
import re
import inspect
import warnings
from .common import get_logger
from .errors import *

# from collections import OrderedDict

__all__ = [
    "to_field",
    "Field",
    "FieldList",
    "FieldFilter",
    "storage_types",
    "analytical_types",
    "distill_aggregate_measures",
    "prepare_key",
    "prepare_aggregation_list",
    "prepare_order_list"
]

"""Abstracted field storage types"""
storage_types = (
        "unknown",  # Unspecified storage type, processing behavior is undefined
        "string",   # names, labels, up to hundreds of hundreds of chars
        "text",     # bigger text storage
        "integer",  # integer numeric types
        "float",    # floating point types
        "boolean",
        "date",
        "array",    # ordered collection type
        "document", # JSON-like object
    )



"""Analytical types used by analytical nodes"""
analytical_types = ("default",  # unspecified or based on storage type
                    "typeless", # not relevant
                    "flag",     # two-element set
                    "discrete", # mostly integer with allowed arithmentic
                    "measure",  # mostly floating point number
                    "nominal",  # unordered set
                    "ordinal"   # ordered set
                    )

"""Mapping between storage types and their respective default analytical
types"""
# NOTE: For the time being, this is private
default_analytical_types = {
                "unknown": "typeless",
                "string": "typeless",
                "text": "typeless",
                "integer": "discrete",
                "float": "measure",
                "date": "typeless",
                "array": "typeless",
                "document": "typeless"
            }

_valid_retype_attributes = ("storage_type",
                     "analytical_type",
                     "concrete_storage_type",
                     "missing_values")

_field_attributes = ["name", "storage_type", "analytical_type",
                     "concrete_storage_type", "size", "missing_values",
                     "label", "info", "origin", "owner"]

def to_field(obj):
    """Converts `obj` to a field object. `obj` can be ``str``, ``tuple``
    (``list``), ``dict`` object or :class:`Field` object. If it is `Field`
    instance, then same object is passed.

    If field is not a `Field` instance, then construction of new field is as follows:

    ``str``:
        `field name` is set

    ``tuple``:
        (`field_name`, `storaget_type`, `analytical_type`), the `field_name` is
        obligatory, rest is optional

    ``dict``
        contains key-value pairs for initializing a :class:`Field` object

    Attributes of a field that are not specified in the `obj` are filled as:
    `storage_type` is set to ``unknown``, `analytical_type` is set to
    ``typeless``
    """

    if isinstance(obj, Field):
        field = obj
    else:
        # Set defaults first
        d = { }

        if isinstance(obj, str):
            d["name"] = obj
        elif isinstance(obj, (list, tuple)):
            d["name"] = obj[0]

            try:
                d["storage_type"] = obj[1]
                try:
                    d["analytical_type"] = obj[2]
                except IndexError:
                    pass
            except IndexError:
                pass

        elif isinstance(obj, dict):
            for attr in _field_attributes:
                if attr in obj:
                    d[attr] = obj[attr]

        else:
            raise ArgumentError("Unable to create field from %s" % (type(obj), ))

        if "analytical_type" not in d:
            storage_type = d.get("storage_type")
            if storage_type:
                deftype = default_analytical_types.get(storage_type)

        field = Field(**d)
    return field

class Field(object):
    """`Field` is a metadata that describes part of data object's structure.
    It might refer to a table column, document key or any other descriptor of
    common data. Fields are identified by name.

    Attributes:

    * `name` - field name.
    * `label` - optional human readable field label that might be used in
      end-user applications.
    * `storage_type` - Normalized data storage type to one of the Bubble's
      basic storage types.
    * `concrete_storage_type` (optional, recommended) - Data store/database
      dependent storage type - this is the real data type descriptor as used
      in a backend system, such as database, where the field comes from or
      where the field is going to be created. If it is `None`, then backend
      tries to use a default type for the field's `storage_type`. Value might
      be any object understandable by the backend that will be "touching" the
      object's data.
    * `analytical_type` - data type from user's perspective. Describes the
      intention how the field is used. Might also be used for restricting some
      functionality based on the type.
    * `size` – optional field size - interpretation of the value is
      related to the `storage_type` and/or `concrete_storaget_type`. For
      example it might be length of text fields or list of array
      dimensions for array type.
    * `missing_values` (optional) - Array of values that represent missing
      values in the dataset for given field
    * `info` – user specific field information, might contain formatting
      information for example

    """
    # TODO: make this public once ownership mechanism is redesigned
    # * `origin` – field or field list from which this field was derived
    # * `owner` – object responsible for creation of this field


    attribute_defaults = {
                "storage_type":"unknown",
                "analytical_type": None
            }

    def __init__(self, *args, **kwargs):
        super(Field,self).__init__()

        remaining = set(_field_attributes)

        for name, value in zip(_field_attributes, args):
            setattr(self, name, value)
            remaining.remove(name)

        for name, value in kwargs.items():
            if name in remaining:
                setattr(self, name, value)
                remaining.remove(name)
            else:
                raise ValueError("Argument %s specified more than once" % name)

        for name in remaining:
            setattr(self, name, self.attribute_defaults.get(name, None))

    def clone(self, **attributes):
        """Clone a field and set attributes"""
        d = self.to_dict()
        d.update(attributes)
        return Field(**d)

    def to_dict(self):
        """Return dictionary representation of the field."""
        d = {}
        for name in _field_attributes:
            d[name] = getattr(self, name)
        return d

    def __copy__(self):
        field = Field(**self.to_dict())
        return field

    def __str__(self):
        """Return field name as field string representation."""
        return self.name

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, self.to_dict())

    def __eq__(self, other):
        if self is other:
            return True
        if not isinstance(other, Field):
            return False

        # TODO: ignore origin
        for name in _field_attributes:
            if getattr(self, name) != getattr(other, name):
                return False
        return True

    def __ne__(self,other):
        return not self.__eq__(other)

    def __hash__(self):
        # Hash should be the same as the one of field's origin
        if isinstance(self.origin, Field):
            return hash(self.origin)
        else:
            return self.name.__hash__()

class FieldList(object):
    """List of fields"""
    def __init__(self, *fields):
        """
        Main description of data object structure. Created from a list of
        :class:`Field` objects from a list of strings, dictionaries or tuples


        How fields are consutrcuted:

        * string: `field name` is set
        * tuple: (`field_name`, `storaget_type`, `analytical_type`), the `field_name` is
          obligatory, rest is optional
        * dict: contains key-value pairs for initializing a :class:`Field` object

        Example: `FieldList('name', 'address')`.

        For strings and in if not explicitly specified in a tuple or a dict case, then following rules
        apply:

        * `storage_type` is set to ``unknown``
        * `analytical_type` is set to ``typeless``
        """
        super(FieldList, self).__init__()

        # FIXME: use OrderedDict (Python 2.7+)
        self._fields = []
        self._field_dict = {}
        self._field_names = []

        if fields:
            # Convert input to Field instances
            # This is convenience, so one can pass list of strsings, for example

            for field in fields:
                self.append(field)

    def append(self, field):
        """Appends a field to the list. This method requires `field` to be
        instance of `Field`"""

        # FIXME: depreciated: FieldList should be immutable
        field = to_field(field)
        self._fields.append(field)
        self._field_dict[field.name] = field
        self._field_names.append(field.name)

    def names(self, indexes=None):
        """Return names of fields in the list.

        :Parameters:
            * `indexes` - list of indexes for which field names should be collected. If set to
              ``None`` then all field names are collected - this is default behaviour.
        """

        if indexes:
            names = [self._field_names[i] for i in indexes]
            return names
        else:
            return self._field_names


    def indexes(self, fields):
        """Return a tuple with indexes of fields from ``fields`` in a data row. Fields
        should be a list of ``Field`` objects or strings.

        This method is useful when it is more desirable to process data as rows (arrays), not as
        dictionaries, for example for performance purposes.
        """

        indexes = [self.index(field) for field in fields]

        return tuple(indexes)

    def index_map(self):
        """Returns a map of field name to field index"""
        return dict( (f, i) for f, i in enumerate(self._field_names))

    def mask(self, fields=None):
        """Return a list representing field selector - which fields are
        selected from a row."""

        sel_names = [str(field) for field in fields]

        mask = [str(name) in sel_names for name in self.names()]
        return mask

    def index(self, field):
        """Return index of a field"""

        try:
            index = self._field_names.index(str(field))
        except ValueError:
            raise NoSuchFieldError("Field list has no field with name '%s'" % str(field))

        return index

    def fields(self, names=None, storage_type=None, analytical_type=None):
        """Return a tuple with fields. `names` specifies which fields are returned. When names is
        ``None`` all fields are returned. `storage_type` or `analytical_type`
        is specified, only fields of that type are returned.
        """

        if not names:
            fields = self._fields
        else:
            fields = [self._field_dict[str(name)] for name in names]

        if storage_type:
            fields = [f for f in fields if f.storage_type == storage_type]

        if analytical_type:
            fields = [f for f in fields if f.analytical_type == analytical_type]

        return FieldList(*fields)

    def aggregated_fields(self, aggregation_list, include_count=True,
                          count_field="record_count"):
        """Returns a `FieldList` containing fields after aggregations of
        measures in the `aggregation_list`. The list should be a list of
        tuples `(field, aggregtion)` and the `field` of the tuple should be in
        the receiving `FieldList`. You can prepare the aggregation list using
        the :func:`prepare_aggregation_list` function.

        Resulting fields are cloned from the original fields and will have
        analytical type set to ``measure``.

        Example:

        >>> agg_list = aggregation_list(['amount', ('discount', 'avg')])
        >>> agg_fields = fields.aggregated_fields(agg_list)

        Will return fields with names: `('amount_sum', 'discount_avg',
        'record_count')`
        """

        agg_fields = FieldList()

        for measure in measures:
            if isinstance(measure, (str, Field)):
                field = str(measure)
                index = fields.index(field)
                aggregate = "sum"
            elif isinstance(measure, (list, tuple)):
                field = measure[0]
                index = fields.index(field)
                aggregate = measure[1]

            field = fields.field(measure)
            field = field.clone(name="%s_%s" % (str(measure), aggregate),
                                analytical_type="measure")
            agg_fields.append(field)

        return agg_fields

    def field(self, ref):
        """Return a field with name `ref` if `ref` is a string, or if it is an
        integer, returns a field at that index."""

        if isinstance(ref, int):
            return self._fields[ref]
        else:
            try:
                return self._field_dict[ref]
            except KeyError:
                raise NoSuchFieldError("Field list has no field with name "
                                        "'%s'" % (ref,) )

    def __len__(self):
        return len(self._fields)

    def __getitem__(self, reference):
        return self.field(reference)

    def __setitem__(self, index, new_field):
        field = self._fields[index]
        del self._field_dict[field.name]
        self._fields[index] = new_field
        self._field_names[index] = new_field.name
        self._field_dict[new_field.name] = new_field

    def __delitem__(self, index):
        field = self._fields[index]
        del self._field_dict[field.name]
        del self._fields[index]
        del self._field_names[index]

    def __iter__(self):
        return self._fields.__iter__()

    def __contains__(self, field):
        if isinstance(field, str):
            return field in self._field_names

        return field in self._fields

    def __iadd__(self, array):
        for field in array:
            self.append(field)

        return self

    def __add__(self, array):
        fields = self.copy()
        fields += array
        return fields

    def __str__(self):
        return "[" + ", ".join(self.names()) + "]"

    def __repr__(self):
        frepr = [repr(field) for field in self._fields]
        return "%s([%s])" % (self.__class__.__name__, ",".join(frepr))

    def __eq__(self, other):
        if not isinstance(other, FieldList):
            return False
        return other._fields == self._fields

    def copy(self):
        """Return a shallow copy of the list.
        """
        return FieldList(*self._fields)

    def clone(self, fields=None, origin=None, owner=None):
        """Creates a copy of the list and copy of the fields.
        """
        fields = self.fields(fields)

        cloned_fields = FieldList()
        for field in fields:
            new_field = copy.copy(field)
            new_field.origin = origin or field
            cloned_fields.append(new_field)

        return cloned_fields

class FieldFilter(object):
    """Filters fields in a stream"""
    # TODO: preserve order of "keep"
    def __init__(self, keep=None, drop=None, rename=None):
        """Creates a field map. `rename` is a dictionary where keys are input
        field names and values are output field names. `drop` is list of
        field names that will be dropped from the stream. If `keep` is used,
        then all fields are dropped except those specified in `keep` list."""
        if drop and keep:
            raise MetadataError("You can nott specify both 'keep' and 'drop' "
                                "options in FieldFilter.")

        super(FieldFilter, self).__init__()

        self.rename = rename or {}

        if drop:
            self.drop = [str(f) for f in drop]
        else:
            self.drop = []

        if keep:
            self.keep = [str(f) for f in keep]
        else:
            self.keep = []

    def filter(self, fields):
        """Map `fields` according to the FieldFilter: rename or drop fields as
        specified. Returns a new FieldList object.

        .. note::

            For each renamed field a new copy is created. Not renamed fields
            are the same as in `fields`. To use filtered fields in a node
            you have to clone the field list.
        """
        output_fields = FieldList()

        # Check whether we have all fields requested
        fields = FieldList(*fields)

        names = fields.names()

        for field in self.keep:
            if field not in names:
                raise NoSuchFieldError(field)

        for field in self.drop:
            if field not in names:
                raise NoSuchFieldError(field)

        for field in fields:
            if field.name in self.rename:
                # Create a copy and rename field if it is mapped
                new_field = copy.copy(field)
                new_field.name = self.rename[field.name]
            else:
                new_field = field

            if (self.drop and field.name not in self.drop) or \
                (self.keep and field.name in self.keep) or \
                not (self.keep or self.drop):
                output_fields.append(new_field)

        return output_fields

    def row_filter(self, fields):
        """Returns an object that will convert rows with structure specified in
        `fields`. You can use the object to filter fields from a row (list,
        array) according to this map.
        """
        return RowFieldFilter(self.field_mask(fields))

    def field_mask(self, fields):
        """Returns a list where ``True`` value is set for field that is selected
        and ``False`` for field that has to be ignored. Selectors of fields can
        be used by `itertools.compress()`. This is the preferred way of field
        filtering.
        """

        selectors = []

        for field in fields:
            flag = (self.drop and field.name not in self.drop) \
                    or (self.keep and field.name in self.keep) \
                    or not (self.keep or self.drop)
            selectors.append(flag)

        return selectors


class RowFieldFilter(object):
    """Class for filtering fields in array"""

    def __init__(self, mask=None):
        """Create an instance of RowFieldFilter. `mask` is a list of indexes
        that are passed to output."""
        super(RowFieldFilter, self).__init__()
        self.mask = mask or []

    def __call__(self, row):
        return self.filter(row)

    def filter(self, row):
        """Filter a `row` according to ``indexes``."""
        return tuple(value for value,mask in zip(row, self.mask) if mask)

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, self.mask)


def distill_aggregate_measures(measures, default_aggregates=None):
    """Normalizes list of measures. Element of the source can be:
    * a string - default aggregations will be used or `sum`` if defaults are
      not provided
    * a field - default aggregations will be used or `sum` if defaults are not
      provided
    * tuple with first element as measure specification and second element
      either aggregation string or list of aggregations

    Returns list of tuples: (`measure`, `aggregate`)
    """
    default_aggregates = default_aggregates or []

    distilled_measures = []
    for measure_aggs in measures:
        # Extract measure and aggregate. If no aggregate is specified,
        # "sum" is assumed.

        if isinstance(measure_aggs, list) or isinstance(measure_aggs, tuple):
            measure = measure_aggs[0]
            aggregates = measure_aggs[1]
            if not (isinstance(aggregates, list) or
                        isinstance(aggregates,tuple)):
                aggregates = [aggregates]
        else:
            measure = measure_aggs
            aggregates = default_aggregates or ["sum"]

        for aggregate in aggregates:
            distilled_measures.append( (measure, aggregate) )

    return distilled_measures


def prepare_key(key):
    """Returns a list of columns for `key`. Key might be a list of fields or
    just one field represented by `Field` object or a string. Examples:

    >>> key = prepare_key(obj, "id")
    >>> key = prepare_key(obj, ["code", "name"])

    If `statement` is not `None`, then columns for the key are returned.
    """

    if isinstance(key, (str, Field)):
        key = (key, )

    return tuple(str(f) for f in key)


##
# Various metadata helpers
#

def prepare_aggregation_list(measures):
    """Coalesces list of measures for aggregation.  `measures` should be a
    list of tuples `(field, aggregtion)`or just fields.  If just a field is
    specified, then `sum` aggregation is assumed. Returns correct list of
    tuples."""

    return prepare_tuple_list(measures, "sum")

def prepare_order_list(fields):
    """Coalesces list of fields for ordering. Accepts: a string, list of
    strings, list of tuples `(field, order)`. Default order is ``asc``."""

    return prepare_tuple_list(fields, "asc")

def prepare_tuple_list(fields, default_value):
    """Coalesces list of fields to list of tuples. Accepts: a string, list of
    strings, list of tuples `(field, value)`. """

    result = []

    if not isinstance(fields, (list, tuple, FieldList)):
        fields = [fields]

    for obj in fields:
        if isinstance(obj, (str, Field)):
            field = str(obj)
            value = "asc"
        elif isinstance(obj, (list, tuple)):
            field, value = obj
        result.append( (field, value) )

    return result
