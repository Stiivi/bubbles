# -*- coding: utf-8 -*-
"""Iterator composing operations."""
import itertools
import functools
import operator
import sys
import datetime
from collections import OrderedDict, namedtuple
from ..metadata import *
from ..common import get_logger
from ..errors import *
from ..operation import operation
from ..objects import *
from ..dev import experimental
from ..prototypes import *

# FIXME: add cheaper version for already sorted data
# FIXME: BasicAuditProbe was removed

__all__ = ()

def unary_iterator(func):
    """Wraps a function that provides an operation returning an iterator.
    Assumes return fields are the same fields as first argument object"""
    @functools.wraps(func)
    def decorator(ctx, obj, *args, **kwargs):
        result = func(ctx, obj, *args, **kwargs)
        return IterableDataSource(result, obj.fields.clone())

    return decorator

#############################################################################
# Metadata Operations

@field_filter.register("rows")
def _(ctx, iterator, keep=None, drop=None, rename=None, filter=None):
    """Filters fields in `iterator` according to the `field_filter`.
    `iterator` should be a rows iterator and `fields` is list of iterator's
    fields."""
    if filter:
        if keep or drop or rename:
            raise OperationError("Either filter or keep, drop, rename should "
                                 "be used")
        else:
            field_filter = filter
    else:
        field_filter = FieldFilter(keep=keep, drop=drop, rename=rename)

    row_filter = field_filter.row_filter(iterator.fields)

    new_iterator = map(row_filter, iterator)
    new_fields = field_filter.filter(iterator.fields)

    return IterableDataSource(new_iterator, new_fields)


#############################################################################
# Row Operations


@filter_by_value.register("rows")
@unary_iterator
def _(ctx, iterator, key, value, discard=False):
    """Select rows where value of `field` belongs to the set of `values`. If
    `discard` is ``True`` then the matching rows are discarded instead
    (operation is inverted)."""

    fields = iterator.fields
    index = fields.index(str(key))

    if discard:
        predicate = lambda row: row[index] != value
    else:
        predicate = lambda row: row[index] == value

    return filter(predicate, iterator)

@filter_by_set.register("rows")
@unary_iterator
def _(ctx, iterator, field, values, discard=False):
    """Select rows where value of `field` belongs to the set of `values`. If
    `discard` is ``True`` then the matching rows are discarded instead
    (operation is inverted)."""
    fields = iterator.fields
    index = fields.index(field)

    # Convert the values to more efficient set
    values = set(values)

    if discard:
        predicate = lambda row: row[index] not in values
    else:
        predicate = lambda row: row[index] in values

    return filter(predicate, iterator)

@filter_by_range.register("rows")
@unary_iterator
def _(ctx, iterator, field, low, high, discard=False):
    """Select rows where value `low` <= `field` <= `high`. If
    `discard` is ``True`` then the matching rows are discarded instead
    (operation is inverted). To check only agains one boundary set the other
    to ``None``. Equivalend of SQL ``BETWEEN``"""

    fields = iterator.fields
    index = fields.index(field)

    if discard:
        if high is None and low is not None:
            predicate = lambda row: not (low <= row[index])
        elif low is None and high is not None:
            predicate = lambda row: not (row[index] <= high)
        else:
            predicate = lambda row: not (low <= row[index] <= high)
    else:
        if high is None and low is not None:
            predicate = lambda row: low <= row[index]
        elif low is None and high is not None:
            predicate = lambda row: row[index] <= high
        else:
            predicate = lambda row: low <= row[index] <= high

    return filter(predicate, iterator)

@filter_not_empty.register("rows")
@unary_iterator
def _(ctx, iterator, field):
    """Select rows where value of `field` is not None"""

    fields = iterator.fields
    index = fields.index(field)

    predicate = lambda row: row[index] is not None

    return filter(predicate, iterator)

@filter_empty.register("rows")
@unary_iterator
def _(ctx, iterator, field):
    """Select rows where value of `field` is None or empty string"""

    fields = iterator.fields
    index = fields.index(field)

    predicate = lambda row: row[index] is None or row[index] == ""

    return filter(predicate, iterator)

@filter_by_predicate.register("rows")
@unary_iterator
def _(ctx, obj, predicate, fields, discard=False,
                        **kwargs):
    """Returns an interator selecting fields where `predicate` is true.
    `predicate` should be a python callable. `arg_fields` are names of fields
    to be passed to the function (in that order). `kwargs` are additional key
    arguments to the predicate function."""

    def iterator(indexes):
        for row in iter(obj):
            values = [row[index] for index in indexes]
            flag = predicate(*values, **kwargs)
            if (flag and not discard) or (not flag and discard):
                yield row

    key = prepare_key(fields)
    indexes = obj.fields.indexes(key)

    return iterator(indexes)


@filter_by_predicate.register("records")
def _(ctx, iterator, predicate, fields, discard=False,
                        **kwargs):
    """Returns an interator selecting fields where `predicate` is true.
    `predicate` should be a python callable. `arg_fields` are names of fields
    to be passed to the function (in that order). `kwargs` are additional key
    arguments to the predicate function."""

    for record in iterator:
        args = [record[str(f)] for f in fields]
        flag = predicate(*args, **kwargs)
        if (flag and not discard) or (not flag and discard):
            yield record


@distinct.register("rows")
def _(ctx, obj, key=None, is_sorted=False):
    """Return distinct `keys` from `iterator`. `iterator` does
    not have to be sorted. If iterator is sorted by the keys and
    `is_sorted` is ``True`` then more efficient version is used."""
    # TODO: remove is_sorted hint, objects should store metadata about
    # that

    def iterator(row_filter):
        if is_sorted:
            last_key = object()

            # FIXME: use itertools equivalent
            for value in obj:
                key_tuple = (row_filter(row))
                if key_tuple != last_key:
                    yield key_tuple

        else:
            distinct_values = set()
            for row in obj:
                # Construct key tuple from distinct fields
                key_tuple = tuple(row_filter(row))
                if key_tuple not in distinct_values:
                    distinct_values.add(key_tuple)
                    yield key_tuple

    fields = obj.fields
    if key:
        key = prepare_key(key)
        row_filter = FieldFilter(keep=key).row_filter(fields)
    else:
        row_filter = FieldFilter().row_filter(fields)

    # Retain original order of fields
    fields = FieldList(*row_filter(obj.fields))

    return IterableDataSource(iterator(row_filter), fields)


@distinct_rows.register("rows")
@unary_iterator
def _(ctx, obj, key=None, is_sorted=False):
    """Return distinct rows based on `key` from `iterator`. `iterator`
    does not have to be sorted. If iterator is sorted by the keys and
    `is_sorted` is ``True`` then more efficient version is used."""
    # TODO: remove is_sorted hint, objects should store metadata about
    # that

    fields = obj.fields
    if key:
        key = prepare_key(key)
        row_filter = FieldFilter(keep=key).row_filter(fields)
    else:
        row_filter = FieldFilter().row_filter(fields)

    if is_sorted:
        last_key = object()

        # FIXME: use itertools equivalent
        for value in obj:
            key_tuple = (row_filter(row))
            if key_tuple != last_key:
                yield row

    else:
        distinct_values = set()
        for row in obj:
            # Construct key tuple from distinct fields
            key_tuple = tuple(row_filter(row))
            if key_tuple not in distinct_values:
                distinct_values.add(key_tuple)
                yield row


@first_unique.register("rows")
@unary_iterator
def _(ctx, iterator, keys=None, discard=False):
    """Return rows that are unique by `keys`. If `discard` is `True` then the
    action is reversed and duplicate rows are returned."""

    # FIXME: add is_sorted version
    # FIXME: use prepare key

    row_filter = FieldFilter(keep=keys).row_filter(iterator.fields)

    distinct_values = set()

    for row in iterator:
        # Construct key tuple from distinct fields
        key_tuple = tuple(row_filter(row))

        if key_tuple not in distinct_values:
            distinct_values.add(key_tuple)
            if not discard:
                yield row
        else:
            if discard:
                # We already have one found record, which was discarded
                # (because discard is true), now we pass duplicates
                yield row


@sample.register("rows")
@unary_iterator
def _(ctx, iterator, value, discard=False, mode="first"):
    """Returns sample from the iterator. If `mode` is ``first`` (default),
    then `value` is number of first records to be returned. If `mode` is
    ``nth`` then one in `value` records is returned."""

    if mode == "first":
        if discard:
            return itertools.islice(iterator, value, None)
        else:
            return itertools.islice(iterator, value)
    elif mode == "nth":
        if discard:
            return discard_nth(iterator, value)
        else:
            return itertools.islice(iterator, None, None, value)
    elif mode == "random":
        raise NotImplementedError("random sampling is not yet implemented")
    else:
        raise Exception("Unknown sample mode '%s'" % mode)


@unary_iterator
def discard_nth_base(ctx, iterator, step):
    """Discards every step-th item from `iterator`"""
    for i, value in enumerate(iterator):
        if i % step != 0:
            yield value

discard_nth.register("rows")(discard_nth_base)
discard_nth.register("records")(discard_nth_base)

@sort.register("rows")
@unary_iterator
def _(ctx, obj, orderby):
    iterator = obj.rows()

    orderby = prepare_order_list(orderby)

    for field, order in reversed(orderby):
        index = obj.fields.index(field)

        if order.startswith("asc"):
            reverse = False
        elif order.startswith("desc"):
            reverse = True
        else:
            raise ValueError("Unknown order %s for column %s") % (order, column)

        iterator = sorted(iterator, key=operator.itemgetter(index),
                                    reverse=reverse)

    return iterator


###
# Simple and naive aggregation in Python

def agg_sum(a, value):
    return a+value

def agg_average(a, value):
    return (a[0]+1, a[1]+value)

def agg_average_finalize(a):
    return a[1]/a[0]

AggregationFunction = namedtuple("AggregationFunction",
                            ["func", "start", "finalize"])
aggregation_functions = {
            "sum": AggregationFunction(agg_sum, 0, None),
            "min": AggregationFunction(min, 0, None),
            "max": AggregationFunction(max, 0, None),
            "average": AggregationFunction(agg_average, (0,0), agg_average_finalize)
        }

@aggregate.register("rows")
def _(ctx, obj, key, measures=None, include_count=True,
      count_field="record_count"):
    """Aggregates measure fields in `iterator` by `keys`. `fields` is a field
    list of the iterator, `keys` is a list of fields that will be used as
    keys. `aggregations` is a list of measures to be aggregated.

    `measures` should be a list of tuples in form (`measure`, `aggregate`).
    See `distill_measure_aggregates()` for how to convert from arbitrary list
    of measures into this form.

    Output of this iterator is an iterator that yields rows with fields that
    contain: key fields, measures (as specified in the measures list) and
    optional record count if `include_count` is ``True`` (default).

    Result is not ordered even the input was ordered.

    .. note:

        This is naïve, pure Python implementation of aggregation. Might not
        fit your expectations in regards of speed and memory consumption for
        large datasets.
    """

    def aggregation_result(keys, aggregates, measure_aggregates):
        # Pass results to output
        for key in keys:
            row = list(key[:])

            key_aggregate = aggregates[key]
            for i, (measure, index, function) in enumerate(measure_aggregates):
                aggregate = key_aggregate[i]
                finalize = aggregation_functions[function].finalize
                if finalize:
                    row.append(finalize(aggregate))
                else:
                    row.append(aggregate)

            if include_count:
                row.append(key_aggregate[-1])

            yield row

    # TODO: create sorted version
    # TODO: include SQL style COUNT(field) to count non-NULL values

    # Coalesce to a list if just one is specified
    keys = prepare_key(key)

    measures = prepare_aggregation_list(measures)

    # Prepare output fields
    out_fields = FieldList()
    out_fields += obj.fields.fields(keys)

    measure_fields = set()
    measure_aggregates = []
    for measure in measures:
        name = measure[0]
        index = obj.fields.index(name)
        aggregate = measure[1]

        measure_aggregates.append( (name, index, aggregate) )
        measure_fields.add(name)

        field = obj.fields.field(name)
        field = field.clone(name="%s_%s" % (name, aggregate),
                            analytical_type="measure")
        out_fields.append(field)

    if include_count:
        out_fields.append(Field(count_field,
                            storage_type="integer",
                            analytical_type="measure"))

    if keys:
        key_selectors = obj.fields.indexes(keys)
    else:
        key_selectors = []

    keys = set()

    # key -> list of aggregates
    aggregates = {}

    for row in obj.rows():
        # Create aggregation key
        key = tuple(row[s] for s in key_selectors)

        # Create new aggregate record for key if it does not exist
        #
        try:
            key_aggregate = aggregates[key]
        except KeyError:
            keys.add(key)
            key_aggregate = []
            for measure, index, function in measure_aggregates:
                start = aggregation_functions[function].start
                key_aggregate.append(start)
            if include_count:
                key_aggregate.append(0)

            aggregates[key] = key_aggregate

        for i, (measure, index, function) in enumerate(measure_aggregates):
            func = aggregation_functions[function].func
            key_aggregate[i] = func(key_aggregate[i], row[index])

        if include_count:
            key_aggregate[-1] += 1

    iterator = aggregation_result(keys, aggregates, measure_aggregates)

    return IterableDataSource(iterator, out_fields)


#############################################################################
# Transpose

@transpose_by.register("rows")
def _(ctx, obj, key, column_field, value_field):

    def iterator():
        nonlocal keep_filter, transpose_filter, transpose_names

        for row in obj:
            keep = keep_filter(row)
            transposed = transpose_filter(row)

            for name, value in zip(transpose_names, transposed):
                yield list(keep) + [name, value]

    key = prepare_key(key)
    out_fields = FieldList()
    out_fields += obj.fields.fields(key)

    # TODO: set type of the field as type of the first transposed field
    out_fields.append(Field(column_field, "string"))
    out_fields.append(Field(value_field))

    keep_filter = FieldFilter(keep=key).row_filter(obj.fields)
    transpose_filter = FieldFilter(drop=key).row_filter(obj.fields)
    transpose_names = transpose_filter(obj.fields.names())

    return IterableDataSource(iterator(), out_fields)

#############################################################################
# Field Operations


@append_constant_fields.register("rows")
def _(ctx, obj, fields, value):
    def iterator(constants):
        for row in obj.rows():
            yield list(row) + constants

    if not isinstance(value, (list, tuple)):
        constants = (value, )
    else:
        constants = value

    output_fields = obj.fields + fields

    return IterableDataSource(iterator(constants), output_fields)


@dates_to_dimension.register("rows")
def _(ctx, obj, fields=None, unknown_date=0):
    def iterator(indexes):
        for row in obj.rows():
            row = list(row)

            for index in indexes:
                row[index] = row[index].strftime("%Y%m%d")

            yield row

    if fields:
        date_fields = obj.fields(fields)
    else:
        date_fields = obj.fields.fields(storage_type="date")

    indexes = obj.fields.indexes(date_fields)

    fields = []
    for field in obj.fields:
        if field in date_fields:
            fields.append(field.clone(storage_type="integer",
                                      concrete_storage_type=None))
        else:
            fields.append(field.clone())

    fields = FieldList(*fields)

    return IterableDataSource(iterator(indexes), fields)


@string_to_date.register("rows")
def _(ctx, obj, fields, fmt="%Y-%m-%dT%H:%M:%S.Z"):
    def iterator(indexes):
        for row in obj.rows():
            row = list(row)
            for index in indexes:
                date_str = row[index]
                value = None
                if date_str:
                    try:
                        value = datetime.datetime.strptime(row[index], fmt)
                    except ValueError:
                        pass

                row[index] = value
            yield row

    date_fields = prepare_key(fields)
    indexes = obj.fields.indexes(date_fields)

    # Prepare output fields
    fields = FieldList()
    for field in obj.fields:
        if str(field) in date_fields:
            fields.append(field.clone(storage_type="date",
                                      concrete_storage_type=None))
        else:
            fields.append(field.clone())

    return IterableDataSource(iterator(indexes), fields)

@split_date.register("rows")
def _(ctx, obj, fields, parts=["year", "month", "day"]):
    """Extract `parts` from date objects"""

    def iterator(indexes):
        for row in obj.rows():
            new_row = []
            for i, value in enumerate(row):
                if i in indexes:
                    for part in parts:
                        new_row.append(getattr(value, part))
                else:
                    new_row.append(value)

            yield new_row

    date_fields = prepare_key(fields)

    indexes = obj.fields.indexes(date_fields)

    # Prepare output fields
    fields = FieldList()
    proto = Field(name="p", storage_type="integer", analytical_type="ordinal")

    for field in obj.fields:
        if str(field) in date_fields:
            for part in parts:
                name = "%s_%s" % (str(field), part)
                fields.append(proto.clone(name=name))
        else:
            fields.append(field.clone())

    return IterableDataSource(iterator(indexes), fields)

@text_substitute.register("rows")
@unary_iterator
def _(ctx, iterator, field, substitutions):
    """Substitute field using text substitutions"""
    # Compile patterns
    fields = iterator.fields
    substitutions = [(re.compile(patt), r) for (patt, r) in subsitutions]
    index = fields.index(field)
    for row in iterator:
        row = list(row)

        value = row[index]
        for (pattern, repl) in substitutions:
            value = re.sub(pattern, repl, value)
        row[index] = value

        yield row

@empty_to_missing.register("rows")
@unary_iterator
@experimental
def _(ctx, iterator, fields=None, strict=False):
    """Converts empty strings into `None` values."""
    if fields:
        if strict:
            fields = iterator.fields.fields(fields)
        else:
            array = []
            for field in fields:
                if field in iterator.fields:
                    array.append(field)
            fields = array
    else:
        fields = iterator.fields.fields(storage_type="string")

    indexes = iterator.fields.indexes(fields)

    for row in iterator:
        row = list(row)
        for index in indexes:
            row[index] = row[index] if row[index] else None
        yield row

@string_strip.register("rows")
@unary_iterator
def _(ctx, iterator, strip_fields=None, chars=None):
    """Strip characters from `strip_fields` in the iterator. If no
    `strip_fields` is provided, then it strips all `string` or `text` storage
    type objects."""

    fields = iterator.fields
    if not strip_fields:
        strip_fields = []
        for field in fields:
            if field.storage_type =="string" or field.storage_type == "text":
                strip_fields.append(field)

    indexes = fields.indexes(strip_fields)

    for row in iterator:
        row = list(row)
        for index in indexes:
            value = row[index]
            if value:
                row[index] = value.strip(chars)
        yield row

@string_split_fixed.register("rows")
def _(ctx, iterato, split_fields=None, new_fields=None, widths=None):

    raise NotImplemented
#############################################################################
# Compositions


@append.register("rows[]")
def _(ctx, objects):
    """Appends iterators"""
    # TODO: check for field equality
    iterators = [iter(obj) for obj in objects]
    iterator = itertools.chain(*iterators)

    return IterableDataSource(iterator, objects[0].fields)



@join_details.register("rows", "rows")
def _(self, master, detail, master_key, detail_key):
    """"Simple master-detail join"""

    def _join_detail_iterator(master, detail, master_key, detail_key):
        """Simple iterator implementation of the left inner join"""

        djoins = []
        detail_map = {}

        # TODO: support compound keys
        detail_index = detail.fields.index(detail_key[0])
        detail_dict = {}

        for row in detail:
            row = list(row)
            key = row.pop(detail_index)
            detail_dict[key] = row

        for master_row in master:
            row = list(master_row)

            master_index = master.fields.index(master_key[0])
            key = master_row[master_index]
            try:
                detail_row = detail_dict[key]
            except KeyError:
                continue
            else:
                yield row + detail_row

    master_key = prepare_key(master_key)
    detail_key = prepare_key(detail_key)

    if len(master_key) > 1 or len(detail_key) > 1:
        raise ArgumentError("Compound keys are not supported yet")

    result = _join_detail_iterator(master, detail, master_key, detail_key)

    # Prepare output fields and columns selection - the selection skips detail
    # columns that are used as key, because they are already present in the
    # master table.

    out_fields = master.fields.clone()
    for field in detail.fields:
        if str(field) not in detail_key:
            out_fields.append(field)

    return IterableDataSource(result, out_fields)


#############################################################################
# Output


@pretty_print.register("records")
def _(ctx, obj, target=None):
    if not target:
        target = sys.stdout

    names = obj.fields.names()
    widths = [len(field.name) for field in obj.fields]
    # Consume data to be pretty-printed
    text_rows = []
    for row in obj.rows():
        line = [str(value) for value in row]
        widths = [max(w, len(val)) for w,val in zip(widths, line)]
        text_rows.append(line)

    format_str = "|"
    for i, field in enumerate(obj.fields):
        width = widths[i]
        if field.storage_type in ("integer", "float"):
            fmt = ">%d" % width
        else:
            fmt = "<%d" % width

        format_str += "{%d:%s}|" % (i, fmt)

    field_borders = [u"-"*w for w in widths]
    border = u"+" + u"+".join(field_borders) + u"+\n"

    format_str += "\n"

    target.write(border)
    header = format_str.format(*names)
    target.write(header)
    target.write(border)

    for row in text_rows:
        target.write(format_str.format(*row))
    target.write(border)

    target.flush()



class BasicAuditProbe(object):
    def __init__(self, key=None, distinct_threshold=10):
        self.field = key
        self.value_count = 0
        self.record_count = 0
        self.value_ratio = 0

        self.distinct_values = set()
        self.distinct_overflow = False
        self.storage_types = set()

        self.null_count = 0
        self.null_value_ratio = 0
        self.null_record_ratio = 0
        self.empty_string_count = 0

        self.distinct_threshold = distinct_threshold

        self.unique_storage_type = None

        self.probes = []

    def probe(self, value):
        storage_type = value.__class__
        self.storage_types.add(storage_type.__name__)

        self.value_count += 1

        # FIXME: check for existence in field.empty_values
        if value is None:
            self.null_count += 1

        if value == '':
            self.empty_string_count += 1

        self._probe_distinct(value)

        for probe in self.probes:
            probe.probe(value)

    def _probe_distinct(self, value):
        """"""
        if self.distinct_overflow:
            return

        # We are not testing lists, dictionaries and object IDs
        storage_type = value.__class__

        if not self.distinct_threshold or \
                len(self.distinct_values) < self.distinct_threshold:
            try:
                self.distinct_values.add(value)
            except:
                # FIXME: Should somehow handle invalid values that can not be added
                pass
        else:
            self.distinct_overflow = True

    def finalize(self, record_count = None):
        if record_count:
            self.record_count = record_count
        else:
            self.record_count = self.value_count

        if self.record_count:
            self.value_ratio = float(self.value_count) / float(self.record_count)
            self.null_record_ratio = float(self.null_count) / float(self.record_count)

        if self.value_count:
            self.null_value_ratio = float(self.null_count) / float(self.value_count)

        if len(self.storage_types) == 1:
            self.unique_storage_type = list(self.storage_types)[0]

    def to_dict(self):
        """Return dictionary representation of receiver."""
        d = {
            "key": self.field,
            "value_count": self.value_count,
            "record_count": self.record_count,
            "value_ratio": self.value_ratio,
            "storage_types": list(self.storage_types),
            "null_count": self.null_count,
            "null_value_ratio": self.null_value_ratio,
            "null_record_ratio": self.null_record_ratio,
            "empty_string_count": self.empty_string_count,
            "unique_storage_type": self.unique_storage_type
        }

        d["distinct_overflow"] = self.distinct_overflow,
        if self.distinct_overflow:
            d["distinct_values"] = []
        else:
            d["distinct_values"] = list(self.distinct_values)

        return d

@basic_audit.register("rows")
def _(ctx, obj, distinct_threshold=100):
    """Performs basic audit of fields in `iterable`. Returns a list of
    dictionaries with keys:

    * `field_name` - name of a field
    * `record_count` - number of records
    * `null_count` - number of records with null value for the field
    * `null_record_ratio` - ratio of null count to number of records
    * `empty_string_count` - number of strings that are empty (for fields of type string)
    * `distinct_values` - number of distinct values (if less than distinct threshold). Set
      to None if there are more distinct values than `distinct_threshold`.
    * `min`
    * `max`
    """

    fields = obj.fields
    out_fields= FieldList(
        Field("key", "string"),
        Field("value_count", "integer"),
        Field("record_count", "integer"),
        Field("value_ratio", "integer"),
        Field("null_count", "integer"),
        Field("null_value_ratio", "number"),
        Field("null_record_ratio", "number"),
        Field("empty_string_count", "integer"),
        Field("unique_storage_type", "string"),
        Field("distinct_overflow", "boolean")
    )

    stats = []
    for field in fields:
        stat = BasicAuditProbe(field.name, distinct_threshold)
        stats.append(stat)

    for row in obj:
        for i, value in enumerate(row):
            stats[i].probe(value)

    result = []
    for stat in stats:
        stat.finalize()
        if stat.distinct_overflow:
            dist_count = None
        else:
            dist_count = len(stat.distinct_values)

        result.append(stat.to_dict())

    return IterableRecordsDataSource(result, out_fields)

# def threshold(value, low, high, bins=None):
#     """Returns threshold value for `value`. `bins` should be names of bins. By
#     default it is ``['low', 'medium', 'high']``
#     """
#
#     if not bins:
#         bins = ['low', 'medium', 'high']
#     elif len(bins) != 3:
#         raise Exception("bins should be a list of three elements")
#
#     if low is None and high is None:
#         raise Exception("low and hight threshold values should not be "
#                         "both none at the same time.")
#


#############################################################################
# Conversions


@as_records.register("rows")
@unary_iterator
def _(ctx, obj):
    """Returns iterator of dictionaries where keys are defined in
    fields."""
    names = [str(field) for field in obj.fields]
    for row in obj:
        yield dict(zip(names, row))


@fetch_all.register("rows")
def _(ctx, obj):
    """Loads all data from the iterable object and stores them in a python
    list. Useful for smaller datasets, not recommended for big data."""

    data = list(obj)

    return RowListDataObject(data, fields=obj.fields)


@as_dict.register("rows")
def _(ctx, obj, key=None, value=None):
    """Returns dictionary constructed from the iterator.  `key` is name of a
    field or list of fields that will be used as a simple key or composite
    key. `value` is a field or list of fields that will be used as values.

    If no `key` is provided, then the first field is used as key. If no
    `value` is provided, then whole rows are used as values.

    Keys are supposed to be unique. If they are not, result might be
    unpredictable.

    .. warning::

        This method consumes whole iterator. Might be very costly on large
        datasets.
    """

    fields = obj.fields

    if not key:
        index = 0
        indexes = None
    elif isinstance(key, (str, Field)):
        index = fields.index(key)
        indexes = None
    else:
        indexes = fields.indexes(key)

    if not value:
        if indexes is None:
            d = dict( (row[index], row) for row in obj)
        else:
            for row in obj:
                key_value = (row[index] for index in indexes)
                d[key_value] = row
    elif isinstance(value, (str, Field)):
        value_index = fields.index(value)
        if indexes is None:
            d = dict( (row[index], row[value_index]) for row in obj)
        else:
            for row in obj:
                key_value = (row[index] for index in indexes)
                d[key_value] = row[value_index]
    else:
        raise NotImplementedError("Specific composite value is not implemented")
    return d


