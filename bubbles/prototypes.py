from .operation import operation

# Operation prototypes – empty operations

#############################################################################
# Metadata Operations

@operation
def field_filter(ctx, iterator, keep=None, drop=None, rename=None,
                 filter=None):
    raise NotImplementedError

@operation
def rename_fields(ctx, obj, rename):
    raise NotImplementedError

@operation
def drop_fields(ctx, obj, drop):
    raise NotImplementedError

@operation
def keep_fields(ctx, obj, keep):
    raise NotImplementedError

@operation
def debug_fields(ctx, obj, label=None):
    raise NotImplementedError

#############################################################################
# Row Filters

@operation
def filter_by_value(ctx, iterator, key, value, discard=False):
    raise NotImplementedError

@operation
def filter_by_set(ctx, iterator, field, values, discard=False):
    raise NotImplementedError

@operation
def filter_by_range(ctx, iterator, field, low, high, discard=False):
    raise NotImplementedError

@operation
def filter_not_empty(ctx, iterator, field):
    raise NotImplementedError

@operation
def filter_empty(ctx, iterator, field):
    raise NotImplementedError

@operation
def filter_by_predicate(ctx, obj, predicate, fields, discard=False,
                        **kwargs):
    raise NotImplementedError

@operation
def distinct(ctx, obj, key=None, is_sorted=False):
    raise NotImplementedError

@operation
def distinct_rows(ctx, obj, key=None, is_sorted=False):
    raise NotImplementedError

@operation
def first_unique(ctx, iterator, keys=None, discard=False):
    raise NotImplementedError

@operation
def sample(ctx, iterator, value, discard=False, mode="first"):
    raise NotImplementedError

@operation
def discard_nth(ctx, iterator, step):
    raise NotImplementedError


#############################################################################
# Ordering

@operation
def sort(ctx, obj, orderby):
    raise NotImplementedError


#############################################################################
# Aggregate

@operation
def aggregate(ctx, obj, key, measures=None, include_count=True,
      count_field="record_count"):
    raise NotImplementedError


#############################################################################
# Field Operations

@operation
def append_constant_fields(ctx, obj, fields, value):
    raise NotImplementedError

@operation
def dates_to_dimension(ctx, obj, fields=None, unknown_date=0):
    raise NotImplementedError

@operation
def string_to_date(ctx, obj, fields, fmt="%Y-%m-%dT%H:%M:%S.Z"):
    raise NotImplementedError

@operation
def split_date(ctx, obj, fields, parts=["year", "month", "day"]):
    raise NotImplementedError

@operation
def text_substitute(ctx, iterator, field, substitutions):
    raise NotImplementedError

@operation
def empty_to_missing(ctx, iterator, fields=None, strict=False):
    raise NotImplementedError

@operation
def string_strip(ctx, iterator, strip_fields=None, chars=None):
    raise NotImplementedError

@operation
def transpose_by(ctx, iterator, key, new_field):
    raise NotImplementedError


#############################################################################
# Compositions

@operation
def append(ctx, objects):
    raise NotImplementedError

@operation(2)
def join_details(self, master, detail, master_key, detail_key):
    raise NotImplementedError


#############################################################################
# Comparison and Inspection

@operation(2)
def added_keys(self, master, detail, src_key, target_key):
    raise NotImplementedError

@operation(2)
def added_rows(self, master, detail, src_key, target_key):
    raise NotImplementedError

@operation(2)
def changed_rows(self, master, detail, dim_key, source_key, fields,
                    version_field):
    raise NotImplementedError

@operation
def count_duplicates(ctx, obj, keys=None, threshold=1,
                     record_count_label="record_count"):
    raise NotImplementedError

@operation
def duplicate_stats(ctx, obj, fields=None, threshold=1):
    raise NotImplementedError

@operation
def nonempty_count(ctx, obj, fields=None):
    raise NotImplementedError

@operation
def distinct_count(ctx, obj, fields=None):
    raise NotImplementedError

@operation
def basic_audit(ctx, iterable, distinct_threshold):
    raise NotImplementedError


#############################################################################
# Loading

@operation(2)
def insert(ctx, source, target):
    raise NotImplementedError

@operation(2)
def load_versioned_dimension(ctx, dim, source, dim_key, fields,
                             version_fields=None, source_key=None):
    raise NotImplementedError


#############################################################################
# Misc

@operation
def pretty_print(ctx, obj, target=None):
    raise NotImplementedError

#############################################################################
# Conversion

@operation
def as_records(ctx, obj):
    raise NotImplementedError

@operation
def fetch_all(ctx, obj):
    raise NotImplementedError

@operation
def as_dict(ctx, obj, key=None, value=None):
    raise NotImplementedError

#############################################################################
# Assertions

@operation
def assert_unique(ctx, obj, key=None):
    raise NotImplementedError

@operation
def assert_contains(ctx, obj, field, value):
    raise NotImplementedError

@operation
def assert_missing(ctx, obj, field, value):
    raise NotImplementedError

@operation
def _(ctx, obj, field, value):
    raise NotImplementedError

@operation
def _(ctx, obj, field, value):
    raise NotImplementedError


