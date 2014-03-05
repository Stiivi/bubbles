# -*- coding: utf-8 -*-

from ..metadata import *
from ..operation import operation
from ..prototypes import *
from ..datautil import guess_type

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

        self.min_len = 0
        self.max_len = 0

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

        try:
            l = len(value)
            self.min_len = min(self.min_len, l)
            self.max_len = max(self.max_len, l)
        except TypeError:
            pass

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
            "field": self.field,
            "value_count": self.value_count,
            "record_count": self.record_count,
            "value_ratio": self.value_ratio,
            "storage_types": list(self.storage_types),
            "null_count": self.null_count,
            "null_value_ratio": self.null_value_ratio,
            "null_record_ratio": self.null_record_ratio,
            "empty_string_count": self.empty_string_count,
            "unique_storage_type": self.unique_storage_type,
            "min_len": self.min_len,
            "max_len": self.max_len
        }

        d["distinct_overflow"] = self.distinct_overflow
        d["distinct_count"] = len(self.distinct_values)
        if self.distinct_overflow:
            d["distinct_values"] = []
        else:
            d["distinct_values"] = list(self.distinct_values)

        return d

@basic_audit.register("rows")
def _(ctx, obj, distinct_threshold=100):

    fields = obj.fields
    out_fields= FieldList(
        Field("field", "string"),
        Field("record_count", "integer"),
        Field("value_ratio", "integer"),
        Field("null_count", "integer"),
        Field("null_value_ratio", "number"),
        Field("null_record_ratio", "number"),
        Field("empty_string_count", "integer"),
        Field("min_len", "integer"),
        Field("max_len", "integer"),
        Field("distinct_count", "integer"),
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

@infer_types.register("rows")
def _(ctx, obj, date_format=None):
    rownum = 0
    probes = defaultdict(set)

    for row in obj:
        rownum += 1
        for i, value in enumerate(row):
            probes[i].add(guess_type(value, date_format))

    keys = list(probes.keys())
    keys.sort()

    types = [probes[key] for key in keys]

    if field_names and len(types) != len(field_names):
        raise Exception("Number of detected fields differs from number"
                        " of fields specified in the header row")
    out_fields = FieldList()
    fields = FieldList(
            Field("field", "string"),
            Field("type", "string")
    )

    result = []
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
        result.append( (name, t) )

    return IterableDataSource(result, out_fields)
