import functools
from ...operation import operation, RetryOperation
from ...metadata import Field, FieldList, FieldFilter
from ...metadata import prepare_aggregation_list
from ...objects import IterableDataSource
from ...errors import *
from .utils import prepare_key, zip_condition, join_on_clause

try:
    import sqlalchemy
    from sqlalchemy import sql
except ImportError:
    from ...common import MissingPackage
    sqlalchemy = MissingPackage("sqlalchemy", "SQL streams", "http://www.sqlalchemy.org/",
                                comment = "Recommended version is > 0.7")
    sql = sqlalchemy

__all__ = (
        "as_records",
        "distinct",
        "distinct_rows",
        "distinct_rows",
        "append",
        "sample",
        "field_filter",
        "filter_by_value",
        "append_constant_fields",
        "count_duplicates",
        "sort",
        "join_details",
        "join_details2",
        "duplicate_stats",
        "nonempty_count",
        "distinct_count",
        "added_keys",
        "added_rows",
        "added_rows2",
        "changed_rows",
        "load_versioned_dimension",
        "dates_to_dimension",
        "assert_unique",
    )



def _unary(func):
    @functools.wraps(func)
    def decorator(ctx, obj, *args, **kwargs):
        result = func(ctx, obj.sql_statement(), *args, **kwargs)
        return obj.clone_statement(statement=result)

    return decorator

#############################################################################
# Metadata Operations

@operation("sql")
def field_filter(ctx, obj, keep=None, drop=None, rename=None):
    """Returns a statement with fields according to the field filter"""
    # TODO: preserve order of "keep" -> see FieldFilter

    field_filter = FieldFilter(keep=keep, drop=drop, rename=rename)

    statement = obj.sql_statement()
    statement = statement.alias("__ff")

    columns = []

    for field in obj.fields:
        name = str(field)
        column = statement.c[name]
        if name in field_filter.rename:
            column = column.label(field_filter.rename[name])
        columns.append(column)

    row_filter = field_filter.row_filter(obj.fields)
    selection = row_filter(columns)

    statement = sql.expression.select(selection, from_obj=statement)
    fields = field_filter.filter(obj.fields)

    result = obj.clone_statement(statement=statement, fields=fields)
    return result


#############################################################################
# Row Operations


@operation("sql")
def filter_by_value(ctx, src, key, value, discard=False):
    """Returns difference between left and right statements"""

    if isinstance(key, (list, tuple)) and \
                    not isinstance(value, (list, tuple)):
        raise ArgumentError("If key is compound then value should be as well")

    cols = src.columns()
    statement = src.sql_statement()
    filter_cols = src.columns(prepare_key(key))
    if len(filter_cols) == 1:
        value = (value, )
    condition = zip_condition(filter_cols, value)
    statement = sql.expression.select(cols, from_obj=statement,
                                        whereclause=condition)

    statement = statement.alias("__value_filter")
    return src.clone_statement(statement=statement)


@operation("sql")
def filter_by_range(ctx, src, field, low, high, discard=False):
    """Filter by range: field should be between low and high."""

    statement = src.sql_statement()
    key_column = statement.c[str(key)]

    if low is not None and high is None:
        cond = key_column >= low
    elif high is not None and low is None:
        cond = key_column < high
    else:
        cond = sql.expression.between(key_column, low, high)

    if discard:
        cond = sql.expression.not_(cond)

    statement = sql.expression.select(statement.columns,
                                      from_obj=statement,
                                      whereclause=cond)

    # TODO: remove this in newer SQLAlchemy version
    statement = statement.alias("__range_filter")
    return src.clone_statement(statement=statement)


@operation("sql")
def filter_by_set(ctx, obj, key, value_set, discard=False):
    raise RetryOperation(["rows"], reason="Not implemented")


@operation("sql")
def filter_not_empty(ctx, obj, field):
    statement = obj.sql_statement()

    column = statement.c[str(field)]
    condition = column != None
    selection = obj.columns()

    statement = sql.expression.select(selection, from_obj=statement,
                                            whereclause=condition)

    return obj.clone_statement(statement=statement)

@operation("sql")
def filter_by_predicate(ctx, obj, fields, predicate, discard=False):
    raise RetryOperation(["rows"], reason="Not implemented")


@operation("sql")
def distinct(ctx, obj, keys=None):
    """Returns a statement that selects distinct values for `keys`"""

    statement = obj.sql_statement()
    if keys:
        keys = prepare_key(keys)
    else:
        keys = obj.fields.names()

    cols = [statement.c[str(key)] for key in keys]
    statement = sql.expression.select(cols, from_obj=statement, group_by=cols)

    fields = obj.fields.fields(keys)

    return obj.clone_statement(statement=statement, fields=fields)


@operation("sql")
def first_unique(ctx, statement, keys=None):
    """Returns a statement that selects whole rows with distinct values
    for `keys`"""
    # TODO: use prepare_key
    raise NotImplementedError

@operation("sql")
@_unary
def sample(ctx, statement, value, mode="first"):
    """Returns a sample. `statement` is expected to be ordered."""

    if mode == "first":
        return statement.select(limit=value)
    else:
        raise RetryOperation(["rows"], reason="Unhandled mode '%s'" % mode)


@operation("sql")
@_unary
def sort(ctx, statement, orderby):
    """Returns a ordered SQL statement. `orders` should be a list of
    two-element tuples `(field, order)`"""

    # Each attribute mentioned in the order should be present in the selection
    # or as some column from joined table. Here we get the list of already
    # selected columns and derived aggregates

    columns = []
    for field, order in orderby:
        column = statement.c[str(field)]
        order = order.lower()
        if order.startswith("asc"):
            column = column.asc()
        elif order.startswith("desc"):
            column = column.desc()
        else:
            raise ValueError("Unknown order %s for column %s") % (order, column)

        columns.append(column)

    statement = sql.expression.select(statement.columns,
                                   from_obj=statement,
                                   order_by=columns)
    return statement

aggregation_functions = {
    "sum": sql.functions.sum,
    "min": sql.functions.min,
    "max": sql.functions.max,
    "count": sql.functions.count
}


@operation("sql")
def aggregate(ctx, obj, key, measures=None, include_count=True,
              count_field="record_count"):

    """Aggregate `measures` by `key`"""

    keys = prepare_key(key)

    if measures:
        measures = prepare_aggregation_list(measures)
    else:
        measures = []

    out_fields = FieldList()
    out_fields += obj.fields.fields(keys)

    statement = obj.sql_statement()
    group = [statement.c[str(key)] for key in keys]

    selection = [statement.c[str(key)] for key in keys]

    for measure, agg_name in measures:
        func = aggregation_functions[agg_name]
        label = "%s_%s" % (str(measure), agg_name)
        aggregation = func(obj.column(measure)).label(label)
        selection.append(aggregation)

        # TODO: make this a metadata function
        field = obj.fields.field(measure)
        field = field.clone(name=label, analytical_type="measure")
        out_fields.append(field)

    if include_count:
        out_fields.append(Field(count_field,
                            storage_type="integer",
                            analytical_type="measure"))

        count =  sql.functions.count(1).label(count_field)
        selection.append(count)

    statement = sql.expression.select(selection,
                                      from_obj=statement,
                                      group_by=group)

    return obj.clone_statement(statement=statement, fields=out_fields)


#############################################################################
# Field Operations

@operation("sql")
def append_constant_fields(ctx, obj, fields, values):
    statement = obj.sql_statement()

    new_fields = obj.fields + FieldList(*fields)

    selection = statement.c
    selection += values

    statement = sql.expression.select(selection, from_obj=statement)
    result = obj.clone_statement(statement=statement, fields=new_fields)

    return result

@operation("sql")
def dates_to_dimension(ctx, obj, fields=None, unknown_date=0):
    """Update all date fields to be date IDs. `unknown_date` is a key to date
    dimension table for unspecified date (NULL in the source).
    `fields` is list of date fields. If not specified, then all date fields
    are considered."""

    statement = obj.sql_statement()
    statement = statement.alias("__to_dim")

    if fields:
        date_fields = obj.fields(fields)
    else:
        date_fields = obj.fields.fields(storage_type="date")

    selection = []
    fields = []
    for field in obj.fields:
        col = statement.c[str(field)]
        if field in date_fields:
            # Safe way
            year = sql.func.extract("year", col)
            month = sql.func.extract("month", col)
            day = sql.func.extract("day", col)
            key = year * 10000 + month * 100 + day
            col = sql.expression.case([ (col != None, key) ],
                    else_=unknown_date)

            # print("CAST TO: %s" % str(col))

            col = col.label(field.name)
            fields.append(field.clone(storage_type="integer",
                                      concrete_storage_type=None))
        else:
            fields.append(field.clone())

        selection.append(col)

    fields = FieldList(*fields)
    statement = sql.expression.select(selection,
                                      from_obj=statement)

    # TODO: mark date fields to be integers
    return obj.clone_statement(statement=statement, fields=fields)

@operation("sql")
def split_date(ctx, obj, fields, parts=["year", "month", "day"]):
    """Extract `parts` from date objects replacing the original date field
    with parts field."""

    statement = obj.sql_statement()
    date_fields = prepare_key(fields)

    # Validate date fields
    for f in obj.fields.fields(date_fields):
        if f.storage_type != "date":
            raise FieldError("Field '%s' is not a date field" % f)

    # Prepare output fields
    fields = FieldList()
    proto = Field(name="p", storage_type="integer", analytical_type="ordinal")

    selection = []

    for field in obj.fields:
        name = str(field)
        col = statement.c[name]
        if name in date_fields:
            for part in parts:
                label = "%s_%s" % (str(field), part)
                fields.append(proto.clone(name=label))
                col_part = sql.expression.extract(part, col)
                col_part = col_part.label(label)
                selection.append(col_part)
        else:
            fields.append(field.clone())
            selection.append(col)

    statement = sql.expression.select(selection,
                                      from_obj=statement)

    return obj.clone_statement(statement=statement, fields=fields)

# @operation("sql")
# def string_to_date(ctx, obj, fields, fmt="%Y-%m-%dT%H:%M:%S.Z"):
# 
#     date_fields = prepare_key(fields)
# 
#     selection = []
#     # Prepare output fields
#     fields = FieldList()
#     for field in obj.fields:
#         col = statement.c[str(field)]
#         if str(field) in date_fields:
#             fields.append(field.clone(storage_type="date",
#                                       concrete_storage_type=None))
# 
#         else:
#             fields.append(field.clone())
#         selection.append(col)
# 
#     return IterableDataSource(iterator(indexes), fields)


#############################################################################
# Compositions

@operation("sql[]")
def append(ctx, objects):
    """Returns a statement with sequentialy concatenated results of the
    `statements`. Statements are chained using ``UNION``."""
    first = objects[0]

    if not all(first.can_compose(o) for o in objects[1:]):
        raise RetryOperation(["rows", "rows[]"],
                             reason="Can not compose")

    statements = [o.sql_statement() for o in objects]
    statement = sqlalchemy.sql.expression.union(*statements)

    return first.clone_statement(statement=statement)


@operation("sql", "sql")
def join_details(ctx, master, detail, master_key, detail_key):
    """Creates a master-detail join using simple or composite keys. The
    columns used as a key in the `detail` object are not included in the
    result.
    """
    # TODO: add left inner, left outer

    if not master.can_compose(detail):
        raise RetryOperation(["rows", "rows"], reason="Can not compose")

    master_key = prepare_key(master_key)
    detail_key = prepare_key(detail_key)

    master_stat = master.sql_statement().alias("__m")
    detail_stat = detail.sql_statement().alias("__d")

    # Prepare the ON left=right ... clause
    onclause = join_on_clause(master_stat, detail_stat, master_key, detail_key)

    # Prepare output fields and columns selection - the selection skips detail
    # columns that are used as key, because they are already present in the
    # master table.

    out_fields = master.fields.clone()
    selection = list(master_stat.columns)
    for field in detail.fields:
        if str(field) not in detail_key:
            out_fields.append(field)
            selection.append(detail_stat.c[str(field)])

    joined = sql.expression.join(master_stat,
                                 detail_stat,
                                 onclause=onclause)

    # Alias the output fields to match the field names
    aliased = []
    for col, field in zip(selection, out_fields):
        aliased.append(col.label(field.name))

    select = sql.expression.select(aliased,
                                from_obj=joined,
                                use_labels=True)

    return master.clone_statement(statement=select, fields=out_fields)

# TODO: depreciated
@operation("sql", "sql[]", name="join_details")
def join_details2(ctx, master, details, joins):
    """Creates left inner master-detail join (star schema) where `master` is an
    iterator if the "bigger" table `details` are details. `joins` is a list of
    tuples `(master, detail)` where the master is index of master key and
    detail is index of detail key to be matched.

    If `inner` is `True` then inner join is performed. That means that only
    rows from master that have corresponding details are returned.

    .. warning::

        all detail iterators are consumed and result is held in memory. Do not
        use for large datasets.
    """
    # TODO: update documentation

    if not details:
        raise ArgumentError("No details provided, nothing to join")

    if not joins:
        raise ArgumentError("No joins specified")

    if len(details) != len(joins):
        raise ArgumentError("For every detail there should be a join "
                            "(%d:%d)." % (len(details), len(joins)))

    if not all(master.can_compose(detail) for detail in details):
        raise RetryOperation(["rows", "rows[]"], reason="Can not compose")

    out_fields = master.fields.clone()

    master_stmt = master.sql_statement().alias("master")
    selection = list(master_stmt.columns)

    joined = master_stmt
    i = 0
    for detail, join in zip(details, joins):
        alias = "detail%s" % i
        det_stmt = detail.sql_statement().alias(alias)
        master_key = join["master"]
        detail_key = join["detail"]

        onclause = master_stmt.c[master_key] == det_stmt.c[detail_key]
        # Skip detail key in the output

        for field, col in zip(detail.fields, det_stmt.columns):
            if str(field) != str(detail_key):
                selection.append(col)
                out_fields.append(field.clone())

        joined = sql.expression.join(joined,
                                     det_stmt,
                                     onclause=onclause)

    aliased = []
    for col, field in zip(selection, out_fields):
        aliased.append(col.label(field.name))

    select = sql.expression.select(aliased,
                                from_obj=joined,
                                use_labels=True)

    return master.clone_statement(statement=select, fields=out_fields)


@operation("sql", "sql")
def added_keys(ctx, src, target, src_key, target_key=None):
    """Returns difference between left and right statements"""

    # FIXME: add composition checking

    if not src.can_compose(target):
        raise RetryOperation(("rows", "sql"), reason="Can not compose")

    target_key = target_key or src_key

    src_cols = src.columns(prepare_key(src_key))
    target_cols = target.columns(prepare_key(target_key))

    src_stat = src.sql_statement()
    target_stat = target.sql_statement()

    src_selection = sql.expression.select(src_cols, from_obj=src_stat)
    target_selection = sql.expression.select(target_cols, from_obj=target_stat)

    diff = src_selection.except_(target_selection)

    return src.clone_statement(statement=diff)


@operation("sql", "sql")
def added_rows(ctx, src, target, src_key, target_key=None):
    diff = ctx.added_keys(src, target, src_key, target_key)

    diff_stmt = diff.sql_statement()
    diff_stmt = diff_stmt.alias("__added_keys")
    src_stmt = src.sql_statement()

    key = prepare_key(src_key)

    src_cols = src.columns(key)
    diff_cols = [diff_stmt.c[f] for f in key]

    cond = zip_condition(src_cols, diff_cols)

    join = sql.expression.join(src_stmt, diff_stmt, onclause=cond)
    join = sql.expression.select(src.columns(), from_obj=join)

    return src.clone_statement(statement=join)


@operation("rows", "sql", name="added_rows")
def added_rows2(ctx, src, target, src_key, target_key=None):

    src_key = prepare_key(src_key)

    if target_key:
        target_key = prepare_key(target_key)
    else:
        target_key = src_key

    statement = target.sql_statement()
    target_cols = target.columns(target_key)

    field_filter = FieldFilter(keep=src_key).row_filter(src.fields)

    def iterator():
        for row in src.rows():
            row_key = field_filter(row)

            cond = zip_condition(target_cols, row_key)

            select = sql.expression.select([sql.func.count(1)],
                                           from_obj=statement,
                                           whereclause=cond)

            result = target.store.execute(select)
            result = list(result)
            if len(result) >= 1 and result[0][0] == 0:
                yield row

    return IterableDataSource(iterator(), fields=src.fields)


# TODO: dimension loading: new values
# TODO: create decorator @target that will check first argument whether it is
# a target or not

@operation("sql", "sql")
def changed_rows(ctx, dim, source, dim_key, source_key, fields, version_field):
    """Return an object representing changed dimension rows.

    Arguments:
        * `dim` – dimension table (target)
        * `source` – source statement
        * `dim_key` – dimension table key
        * `source_key` – source table key
        * `fields` – fields to be compared for changes
        * `version_field` – field that is optionally checked to be empty (NULL)

    """
    src_columns = source.columns(fields)
    dim_columns = dim.columns(fields)
    src_stmt = source.sql_statement()
    dim_stmt = dim.sql_statement()

    # 1. find changed records
    # TODO: this might be a separate operation

    join_cond = zip_condition(dim.columns(prepare_key(dim_key)),
                              source.columns(prepare_key(source_key)))
    join = sql.expression.join(src_stmt, dim_stmt, onclause=join_cond)

    change_cond = [ d != s for d, s in zip(dim_columns, src_columns) ]
    change_cond = sql.expression.or_(*change_cond)

    if version_field:
        version_column = dim.column(version_field)
        change_cond = sql.expression.and_(change_cond, version_column == None)

    join = sql.expression.select(source.columns(),
                                 from_obj=join,
                                 whereclause=change_cond)

    return source.clone_statement(statement=join)

#############################################################################
# Loading

# TODO: continue here
@operation("sql_table", "sql")
def load_versioned_dimension(ctx, dim, source, dim_key, fields,
                             version_fields=None, source_key=None):
    """Type 2 dimension loading."""
    # Now I need to stay in the same kernel!

    new = added_rows(dim, source)


#############################################################################
# Auditing


@operation("sql")
def count_duplicates(ctx, obj, keys=None, threshold=1,
                       record_count_label="record_count"):
    """Returns duplicate rows based on `keys`. `threshold` is lowest number of
    duplicates that has to be present to be returned. By default `threshold`
    is 1. If no keys are specified, then all columns are considered."""

    if not threshold or threshold < 1:
        raise ValueError("Threshold should be at least 1 "
                         "meaning 'at least one duplcate'.")

    statement = obj.sql_statement()

    count_field = Field(record_count_label, "integer")

    if keys:
        keys = prepare_key(keys)
        group = [statement.c[str(field)] for field in keys]
        fields = list(keys)
        fields.append(count_field)
        out_fields = FieldList(*fields)
    else:
        group = list(statement.columns)
        out_fields = obj.fields.clone() + FieldList(count_field)

    counter = sqlalchemy.func.count("*").label(record_count_label)
    selection = group + [counter]
    condition = counter > threshold

    statement = sql.expression.select(selection,
                                   from_obj=statement,
                                   group_by=group,
                                   having=condition)

    result = obj.clone_statement(statement=statement, fields=out_fields)
    return result

@operation("sql_statement")
def duplicate_stats(ctx, obj, fields=None, threshold=1):
    """Return duplicate statistics of a `statement`"""
    count_label = "__record_count"
    dups = duplicates(obj, threshold, count_label)
    statement = dups.statement
    statement = statement.alias("duplicates")

    counter = sqlalchemy.func.count("*").label("record_count")
    group = statement.c[count_label]
    result_stat = sqlalchemy.sql.expression.select([counter, group],
                                              from_obj=statement,
                                              group_by=[group])

    fields = dups.fields.clone()
    fields.add(count_label)

    result = obj.clone_statement(statement=result_stat, fields=fields)
    return result

@operation("sql")
def nonempty_count(ctx, obj, fields=None):
    """Return count of empty fields for the object obj"""

    # FIXME: add fields
    # FIXME: continue here

    statement = obj.sql_statement()
    statement = statement.alias("empty")

    if not fields:
        fields = obj.fields
    fields = prepare_key(fields)

    cols = [statement.c[f] for f in fields]
    selection = [sqlalchemy.func.count(col) for col in cols]
    statement = sqlalchemy.sql.expression.select(selection,
                                                  from_obj=statement)

    out_fields = obj.fields.fields(fields)
    return obj.clone_statement(statement=statement, fields=out_fields)

    # field, key, key, key, empty_count

@operation("sql")
def distinct_count(ctx, obj, fields=None):
    """Return count of empty fields for the object obj"""

    # FIXME: add fields
    # FIXME: continue here

    statement = obj.sql_statement()
    statement = statement.alias("distinct_count")

    if not fields:
        fields = obj.fields
    fields = prepare_key(fields)

    cols = [statement.c[f] for f in fields]
    selection = []
    for col in cols:
        c = sqlalchemy.func.count(sqlalchemy.func.distinct(col))
        selection.append(c)

    statement = sqlalchemy.sql.expression.select(selection,
                                                  from_obj=statement)

    out_fields = obj.fields.fields(fields)
    return obj.clone_statement(statement=statement, fields=out_fields)


#############################################################################
# Assertions


@operation("sql")
def assert_unique(ctx, obj, key=None):
    """Checks whether the receiver has unique values for `key`. If `key` is
    not specified, then all fields from `obj` are considered."""

    statement = obj.sql_statement().alias("__u")

    if key:
        key = prepare_key(key)
        group = [statement.c[field] for field in key]
    else:
        group = list(statement.columns)

    counter = sqlalchemy.func.count("*").label("duplicate_count")
    selection = [counter]
    condition = counter > 1

    statement = sql.expression.select(selection,
                                       from_obj=statement,
                                       group_by=group,
                                       having=condition,
                                       limit=1)

    result = list(obj.store.execute(statement))

    if len(result) != 0:
        raise ProbeAssertionError

    return obj

@operation("sql")
def assert_contains(ctx, obj, field, value):
    statement = obj.sql_statement().alias("__u")
    column = statement.c[str(field)]

    condition = column == value
    selection = [1]

    statement = sql.expression.select(selection,
                                       from_obj=statement,
                                       group_by=group,
                                       having=condition,
                                       limit=1)

    result = list(obj.store.execute(statement))
    if len(result) != 1:
        raise ProbeAssertionError

    return obj

@operation("sql")
def assert_missing(ctx, obj, field, value):
    # statement = obj.sql_statement().alias("__u")
    statement = obj.sql_statement()
    column = statement.c[str(field)]

    condition = column == value
    selection = [1]

    statement = sql.expression.select(selection,
                                       from_obj=statement,
                                       whereclause=condition,
                                       limit=1)

    result = list(obj.store.execute(statement))
    if len(result) != 0:
        raise ProbeAssertionError

    return obj

#############################################################################
# Conversions


@operation("sql")
def as_records(ctx, obj):
    """Return object with records representation."""
    # SQL Alchemy result can be used as both - records or rows, so we just
    # return the object:

    return obj


#############################################################################
# Loading

@operation("sql", "sql")
def insert(ctx, source, target):
    if not target.can_compose(src):
        raise RetryOperation(["rows"])

    # Flush all data that were added through append() to preserve
    # insertion order (just in case)
    target.flush()

    # Preare INSERT INTO ... SELECT ... statement
    statement = InsertFromSelect(target.table, source.selectable())

    target.store.execute(statement)

    return target

@operation("rows", "sql")
def insert(ctx, source, target):

    if len(source.fields) > len(target.fields):
         raise OperationError("Number of source fields %s is greater than "
                              "number of target fields %s" % (len(source.fields),
                                                             len(target.fields)))

    missing = set(source.fields.names()) - set(target.fields.names())
    if missing:
        raise OperationError("Source contains fields that are not in the "
                "target: %s" % (missing, ))

    indexes = []
    for name in target.fields.names():
        if name in source.fields:
            indexes.append(source.fields.index(name))
        else:
            indexes.append(None)

    target.flush()
    for row in source.rows():
        row = [row[i] if i is not None else None for i in indexes]
        target.append(row)

    target.flush()

    return target

