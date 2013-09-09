# -*- coding: utf-8 -*-
from ...objects import *
from ...errors import *
from ...common import get_logger
from ...metadata import Field, FieldList
from ...stores import DataStore

__all__ = (
        "SQLDataStore",
        "SQLTable",
        "SQLStatement",
        "reflect_fields"
    )

try:
    import sqlalchemy
    import sqlalchemy.sql as sql
    # (sql type, storage type, analytical type)
    _sql_to_bubbles_types = (
        (sqlalchemy.types.UnicodeText, "text", "typeless"),
        (sqlalchemy.types.Text, "text", "typeless"),
        (sqlalchemy.types.Unicode, "string", "set"),
        (sqlalchemy.types.String, "string", "set"),
        (sqlalchemy.types.Integer, "integer", "discrete"),
        (sqlalchemy.types.Numeric, "float", "range"),
        (sqlalchemy.types.DateTime, "date", "typeless"),
        (sqlalchemy.types.Date, "date", "typeless"),
        (sqlalchemy.types.Time, "unknown", "typeless"),
        (sqlalchemy.types.Interval, "unknown", "typeless"),
        (sqlalchemy.types.Boolean, "boolean", "flag"),
        (sqlalchemy.types.Binary, "unknown", "typeless")
    )

    concrete_sql_type_map = {
        "string": sqlalchemy.types.Unicode,
        "text": sqlalchemy.types.UnicodeText,
        "date": sqlalchemy.types.Date,
        "time": sqlalchemy.types.DateTime,
        "integer": sqlalchemy.types.Integer,
        "float": sqlalchemy.types.Numeric,
        "boolean": sqlalchemy.types.SmallInteger
    }

    from sqlalchemy.sql.expression import Executable, ClauseElement
    from sqlalchemy.ext.compiler import compiles

    class CreateTableAsSelect(Executable, ClauseElement):
        def __init__(self, table, select):
            self.table = table
            self.select = select

    @compiles(CreateTableAsSelect)
    def visit_create_table_as_select(element, compiler, **kw):
        return "CREATE TABLE %s AS (%s)" % (
            element.table,
            compiler.process(element.select)
        )

    class InsertFromSelect(Executable, ClauseElement):
        _execution_options = \
            Executable._execution_options.union({'autocommit': True})

        def __init__(self, table, select):
            self.table = table
            self.select = select

    @compiles(InsertFromSelect)
    def visit_insert_from_select(element, compiler, **kw):
        # TODO: there is different syntax for postgresql: uses (query)
        preparer = compiler.dialect.preparer(compiler.dialect)
        colnames = [preparer.format_column(c) for c in element.select.columns]
        collist = ", ".join(colnames)
        return "INSERT INTO %s (%s) %s" % (
            compiler.process(element.table, asfrom=True),
            collist,
            compiler.process(element.select)
        )

except ImportError:
    from ...common import MissingPackage
    sqlalchemy = MissingPackage("sqlalchemy", "SQL streams", "http://www.sqlalchemy.org/",
                                comment = "Recommended version is > 0.7")
    _sql_to_bubbles_types = ()
    concrete_sql_type_map = {}

# Length of string data types such as varchar in dialects that require length
# to be specified. This value is used when no `Field.size` is specified.
DEFAULT_STRING_LENGTH = 126


def concrete_storage_type(field, type_map={}, dialect=None):
    """Derives a concrete storage type for the field based on field conversion
       dictionary"""

    concrete_type = field.concrete_storage_type

    if not isinstance(concrete_type, sqlalchemy.types.TypeEngine):
        if type_map:
            concrete_type = type_map.get(field.storage_type)

        if not concrete_type:
            concrete_type = concrete_sql_type_map.get(field.storage_type)

        # Account for type specific options, like "length"
        if (concrete_type == sqlalchemy.types.Unicode):
            # TODO: add all dialects that require length for strings
            if not field.size and dialect in ("mysql", ):
                length = DEFAULT_STRING_LENGTH
            else:
                length = field.size

            concrete_type = sqlalchemy.types.Unicode(length=length)

        if not concrete_type:
            raise ValueError("unable to find concrete storage type for field '%s' "
                                "of type '%s' (concrete: %s)" \
                            % (field.name, field.storage_type,
                                field.concrete_storage_type))

    return concrete_type

"""List of default shared stores."""
_default_stores = {}

def reflect_fields(selectable):
    """Get fields from a table. Field types are normalized to the bubbles
    data types. Analytical type is set according to a default conversion
    dictionary."""

    fields = []

    for column in selectable.columns:
        field = Field(name=column.name)
        field.concrete_storage_type = column.type

        for conv in _sql_to_bubbles_types:
            if issubclass(column.type.__class__, conv[0]):
                field.storage_type = conv[1]
                field.analytical_type = conv[2]
                break

        if field.storage_type is not None:
            field.storaget_tpye = "unknown"

        if field.analytical_type is not None:
            field.analytical_type = "unknown"

        fields.append(field)

    return FieldList(*fields)

def default_store(url=None, connectable=None, schema=None):
    """Gets a default store for connectable or URL. If store does not exist
    one is created and added to shared default store pool."""

    if url and connectable:
        raise ArgumentError("Only one of URL or connectable should be " \
                            "specified, not both")

    if url:
        try:
            store = _default_stores[url]
        except KeyError:
            store = SQLDataStore(url=url, schema=schema)
            _default_stores[url] = store
            _default_stores[store.connectable] = store
    else:
        try:
            store = _default_stores[connectable]
        except KeyError:
            store = SQLDataStore(connectable=connectable)
            _default_stores[store.connectable] = store

    return store

def _postgres_copy_from(self, connection, table, stream, is_csv=True,
                         null_string=None, delimiter=None):
    """Loads data from file-like object `stream` into a `table`."""
    connection = engine.raw_connection()

    cursor = connection.cursor()

    preparer = sqlalchemy.sql.compiler.IdentifierPreparer("postgres")
    table_name = format_table(table, use_schema=True)

    options = []

    if is_csv:
        options.append("CSV")
    if null_string:
        options.append("NULL '%s'" % null_string)
    if delimiter:
        options.append("DELIMITER '%s'" % delimiter)

    if options:
        options_string = "WITH "+" ".join(options)
    else:
        options_string = ""

    sql = "COPY %s FROM STDIN %s" % (table_name, options_string)

    cursor.copy_expert(sql, stream)
    cursor.close()
    connection.commit()

class SQLDataStore(DataStore):
    """Holds context of SQL store operations."""

    __identifier__ = "sql"

    def __init__(self, url=None, connectable=None, schema=None,
            concrete_type_map=None, sqlalchemy_options=None):
        """Opens a SQL data store.

        * `url` – connection URL (see SQLAlchemy documentation for more
          information)
        * `connectable` – a connection or an engine
        * `schema` – default database schema
        * `concrete_Type_map` – a dictionary where keys are generic storage
          types and values are concrete storage types
        * `sqlalchemy_options` – options passed to `create_engine()`

        Either `url` or `connectable` should be specified, but not both.
        """

        if not url and not connectable:
            raise AttributeError("Either url or connectable should be provided" \
                                 " for SQL data source")

        super(SQLDataStore, self).__init__()

        if connectable:
            self.connectable = connectable
            self.should_close = False
        else:
            sqlalchemy_options = sqlalchemy_options or {}
            self.connectable = sqlalchemy.create_engine(url,
                    **sqlalchemy_options)
            self.should_close = True

        self.concrete_type_map = concrete_type_map or concrete_sql_type_map

        self.metadata = sqlalchemy.MetaData(bind=self.connectable)
        self.schema = schema
        self.logger = get_logger()

    def clone(self, schema=None, concrete_type_map=None):
        store = SQLDataStore(connectable=self.connectable,
                             schema=schema or self.schema,
                             concrete_type_map=concrete_type_map or
                                                     self.concrete_type_map
                             )
        return store

    def objects(self, names=None):
        """Return list of tables and views.

        * `names`: only objects with given names are returned
        """

        self.metadata.reflect(schema=self.schema, views=True, only=names)
        tables = self.metadata.sorted_tables

        objects = []
        for table in tables:
            obj = SQLTable(table=table, schema=self.schema, store=self)
            objects.append(obj)

        return objects

    def get_object(self, name):
        """Returns a `SQLTable` object for a table with name `name`."""

        obj = SQLTable(table=name, schema=self.schema, store=self)
        return obj

    def statement(self, statement, fields=None):
        """Returns a statement object belonging to this store"""
        if not fields:
            fields = reflect_fields(statement)

        return SQLStatement(statement=statement,
                            store=self,
                            fields=fields,
                            schema=self.schema)

    def create(self, name, fields, replace=False, from_obj=None, schema=None,
               id_column=None):
        """Creates a table and returns `SQLTable`. See `create_table()`
        for more information"""
        table = self.create_table(name, fields=fields, replace=replace,
                                  from_obj=from_obj, schema=schema,
                                  id_column=id_column)
        return SQLTable(store=self, table=table, fields=fields,
                                schema=schema)

    def create_table(self, name, fields, replace=False, from_obj=None, schema=None,
               id_column=None):
        """Creates a new table.

        * `fields`: field list for new columns
        * `replace`: if table exists, it will be dropped, otherwise an
          exception is raised
        * `from_obj`: object with SQL selectable compatible representation
          (table or statement)
        * `schema`: schema where new table is created. When ``None`` then
          store's default schema is used.
        """

        schema = schema or self.schema

        table = self.table(name, schema, autoload=False)
        if table.exists():
            if replace:
                self.delete(name, schema)
                # Create new table object
                table = self.table(name, schema, autoload=False)
            else:
                schema_str = " (in schema '%s')" % schema if schema else ""
                raise ObjectExistsError("Table %s%s already exists" % (table, schema_str))

        if from_obj:
            if id_column:
                raise ArgumentError("id_column should not be specified when "
                                    "creating table from another object")

            return self._create_table_from(table, from_obj)
        elif id_column:
            sequence_name = "seq_%s_%s" % (name, id_column)
            sequence = sqlalchemy.schema.Sequence(sequence_name,
                                                  optional=True)
            col = sqlalchemy.schema.Column(id_column,
                                           sqlalchemy.types.Integer,
                                           sequence, primary_key=True)
            table.append_column(col)

        for field in fields:
            concrete_type = concrete_storage_type(field, self.concrete_type_map)
            if field.name == id_column:
                sequence_name = "seq_%s_%s" % (name, id_column)
                sequence = sqlalchemy.schema.Sequence(sequence_name,
                                                      optional=True)
                col = sqlalchemy.schema.Column(id_column,
                                           concrete_type,
                                           sequence, primary_key=True)
            else:
                col = sqlalchemy.schema.Column(field.name, concrete_type)

            table.append_column(col)

        table.create()

        return table

    def _create_table_from(table, from_obj):
        """Creates a table using ``CREATE TABLE ... AS SELECT ...``. The
        `from_obj` should have SQL selectable compatible representation."""

        source = from_obj.selectable()
        statement = CreateTableAsSelect(table, source)
        self.connectable.execute(statement)
        return self.table(name=table, autoload=True)

    def delete(self, name, schema):
        """Drops table"""
        schema = schema or self.schema
        table = self.table(name, schema, autoload=False)
        if not table.exists():
            raise Exception("Trying to delete table '%s' that does not exist" \
                                                                    % name)
        table.drop(checkfirst=False)
        self.metadata.drop_all(tables=[table])
        self.metadata.remove(table)

    def table(self, table, schema=None, autoload=True):
        """Returns a table with `name`. If schema is not provided, then
        store's default schema is used."""
        if table is None:
            raise Exception("Table name should not be None")
        if isinstance(table, sqlalchemy.schema.Table):
            return table

        schema = schema or self.schema

        try:
            return sqlalchemy.Table(table, self.metadata,
                                autoload=autoload, schema=schema)
        except sqlalchemy.exc.NoSuchTableError:
            if schema:
                slabel = " in schema '%s'" % schema
            else:
                slabel = ""

            raise NoSuchObjectError("Unable to find table '%s'%s" % \
                                    (table, slabel))

    def execute(self, statement, *args, **kwargs):
        """Executes `statement` in store's connectable"""
        # TODO: Place logging here
        # self.logger.debug("SQL: %s" % str(statement))
        return self.connectable.execute(statement, *args, **kwargs)

class SQLDataObject(DataObject):
    _bubbles_info = { "abstract": True }

    def __init__(self, store=None, schema=None):
        """Initializes new `SQLDataObject`. `store` might be a `SQLDataStore`
        object, a URL string or SQLAlchemy connectable object. If it is
        not a concrete store, then default store for that URL/connectable is
        created and/or reused if already exists. `schema` is a database schema
        for this object. It might be different from `store`'s schema."""

        if isinstance(store, SQLDataStore):
            self.store = store
        elif isinstance(store, str):
            self.store = default_store(url=store, schema=schema)
        else:
            self.store = default_store(connectable=store, schema=schema)

        self.schema = schema

    def can_compose(self, obj):
        """Returns `True` if `obj` can be composed with the receiver – that
        is, whether the target object is also a SQL object within same
        engine"""

        if not isinstance(obj, SQLDataObject):
            return False
        if not hasattr(obj, "store"):
            return False
        if self.store.connectable == obj.store.connectable:
            return True
        else:
            return False

    def records(self):
        # SQLAlchemy result is dict-like object where values can be accessed
        # by field names as well, so we just return the same iterator
        return self.rows()

    def __iter__(self):
        return self.rows()

    def is_consumable(self):
        return False

    def clone_statement(self, statement=None, fields=None):
        """Clone statement representation from the receiver. If `statement` or
        `fields` are not specified, then they are copied from the receiver.

        Use this method in operations to create derived statements from the
        receiver.
        """
        if statement is None:
            statement = self.sql_statement()

        fields = fields or self.fields.clone()
        obj = SQLStatement(statement, self.store, fields=fields,
                                    schema=self.schema)
        return obj

class SQLStatement(SQLDataObject):
    """Object representing a SQL statement (from SQLAlchemy)."""

    _bubbles_info = {
        "attributes": [
            {"name":"statement", "description": "SQL statement"},
            {"name":"store", "description":"SQL data store"},
            {"name":"schema", "description":"default schema"},
            {"name":"fields", "description":"statement fields (columns)"}
        ],
        "requirements": ["sqlalchemy"]
    }

    def __init__(self, statement, store, fields=None, schema=None):
        """Creates a relational database data object.

        Attributes:

        * `statement`: SQL statement to be used as a data source
        * `store`: SQL data store the object belongs to. Might be a
          `SQLDataStore` instance, URL string or a connectable object.
        * `schema` - database schema, if different than schema of `store`
        * `fields` - list of fields that override automatic field reflection
          from the statement

        If `store` is not provided, then default store is used for given
        connectable or URL. If no store exists, one is created.
        """

        super(SQLStatement, self).__init__(store=store, schema=schema)

        self.statement = statement
        try:
            self.name = statement.name
        except AttributeError:
            schema_label = "%s_" % schema if schema else ""
            self.name = "anonymous_statement_%s%d" % (schema_label, id(self))

        self.statement = statement

        if fields:
            self.fields = fields
        else:
            self.fields = reflect_fields(statement)

    def as_target(self):
        raise DataObjectError("SQL statement (%s) can not be used "
                                "as target object" % self.name)
    def __len__(self):
        """Returns number of rows selected by the statement."""
        cnt = sqlalchemy.sql.func.count(1)
        statement = sql.expression.select([cnt], from_obj=self.statement)

        return self.store.connectable.scalar(statement)

    def rows(self):
        return iter(self.store.execute(self.statement))

    def selectable(self):
        return self.statement

    def sql_statement(self):
        return self.statement

    def representations(self):
        """Return list of possible object representations"""
        return ["sql", "rows", "records"]

    def columns(self, fields=None):
        """Returns Column objects for `fields`. If no `fields` are specified,
        then all columns are returned"""

        if fields is None:
            return self.statement.columns

        cols = [self.statement.c[str(field)] for field in fields]

        return cols

    def column(self, field):
        """Returns a column for field"""
        return self.statement.c[str(field)]

class SQLTable(SQLDataObject):
    """Object representing a SQL database table or view (from SQLAlchemy)."""

    _bubbles_info = {
        "attributes": [
            {"name":"table", "description": "table name"},
            {"name":"store", "description":"SQL data store"},
            {"name":"schema", "description":"default schema"},
            {"name":"fields", "description":"statement fields (columns)"},

            {"name":"buffer_size", "description":"size of insert buffer"},
            {
                "name":"create",
                "description":"flag whether table is created",
                "type":"boolean"
            },
            {
                "name":"truncate",
                "description":"flag whether table is truncated",
                "type":"boolean"
            },
            {
                "name":"replace",
                "description":"flag whether table is replaced when created",
                "type":"boolean"
            },
        ],
        "requirements": ["sqlalchemy"]
    }

    def __init__(self, table, store, fields=None, schema=None,
                 create=False, replace=False, truncate=False,
                 id_key_name=None, buffer_size=1024):
        """Creates a relational database data object.

        Attributes:

        * `table`: table name
        * `store`: SQL data store the object belongs to
        * `fields`: fieldlist for a new table
        * `truncate`: if `True` table will be truncated upon initiaization
        * `create`: if `True` then table will be created during object
          initialization. Default is `False` and exception is raised when
          table does not exist.
        * `replace`:  if `True` and `create` is requested, then existing table
          will be dropped and created as new.
        * `id_key_name`: name of the auto-increment key during table creation.
            If specified, then key column is created, otherwise no key column
            is created.
        * `buffer_size`: size of buffer for table INSERTs - how many records
          are collected before they are inserted using multi-insert statement.
          Default is 1000.

        """

        super(SQLTable, self).__init__(store=store, schema=schema)

        self.fields = None

        if create:
            if fields is None:
                raise ArgumentError("No fields specified for new table")

            self.fields = fields
            self.table = self.store.create_table(table, self.fields,
                                                 replace=replace,
                                                 schema=schema,
                                                 id_column=id_key_name)
            # FIXME: reflect concrete storage types
        else:
            if isinstance(table, str):
                self.table = self.store.table(table, schema=schema, autoload=True)
            else:
                self.table = table

            if not self.fields:
                self.fields = reflect_fields(self.table)

        self.name = self.table.name

        if truncate:
            self.table.delete().execute()

        self._field_names = self.fields.names()

        # Bulk INSERT buffer (if backends supports bulk inserts)
        self.buffer_size = buffer_size
        self._insert_buffer = []
        self.insert_statement = self.table.insert()
        # SQL Statement representation

    def rows(self):
        return iter(self.table.select().execute())

    def representations(self):
        """Return list of possible object representations"""
        return ["sql_table", "sql", "records", "rows"]

    def selectable(self):
        return self.table.select()

    def sql_statement(self):
        return self.table

    def sql_table(self):
        return self.table

    def __len__(self):
        """Returns number of rows in a table"""
        statement = self.table.count()
        result = self.store.connectable.scalar(statement)
        return result

    def truncate(self):
        if self.table is None:
            raise RepresentationError("Can not truncate: "
                                      "SQL object is a statement not a table")
        self.store.execute(self.table.delete())

    def append(self, row):
        # FIXME: remove this once SQLAlchemy will allow list based multi-insert
        row = dict(zip(self._field_names, row))

        self._insert_buffer.append(row)
        if len(self._insert_buffer) >= self.buffer_size:
            self.flush()

    def flush(self):
        if self._insert_buffer:
            self.store.execute(self.insert_statement, self._insert_buffer)
            self._insert_buffer = []

    def append_from(self, obj):
        """Appends data from object `obj` which might be a `DataObject`
        instance or an iterable. If `obj` is a `DataObject`, then it should
        have one of following representations, listed in order of preferrence:

        * sql_table
        * sql_statement
        * rows

        If the `obj` is just an iterable, then it is treated as `rows`
        representation of data object.

        `flush()` is called:

        * before SQL insert from a table or a statement
        * after insert of all rows of `rows` representation
        """

        # TODO: depreciate this in favor of the insert() operation
        reprs = obj.representations()

        if self.can_compose(obj):
            self.store.logger.debug("append_from: composing into %s" %
                                                                self.name)
            # Flush all data that were added through append() to preserve
            # insertion order (just in case)
            self.flush()

            # Preare INSERT INTO ... SELECT ... statement
            source = obj.selectable()
            statement = InsertFromSelect(self.table, source)
            self.store.execute(statement)

        elif "rows" in reprs:
            self.store.logger.debug("append_from: appending rows into %s" %
                                                                self.name)
            for row in obj.rows():
                self.append(row)

            # Clean-up after bulk insertion
            self.flush()

        else:
            raise RepresentationError(
                            "Incopatible representations '%s'", (reprs, ) )

    def probe(self, probeset):
        return probe_table(self, probeset)

    def columns(self, fields=None):
        """Returns Column objects for `fields`. If no `fields` are specified,
        then all columns are returned"""

        if fields is None:
            return self.table.columns

        cols = [self.table.c[str(field)] for field in fields]

        return cols

    def column(self, field):
        """Returns a column for field"""
        return self.table.c[str(field)]

