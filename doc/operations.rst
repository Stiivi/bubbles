##########
Operations
##########

There are several kinds of operations:

* Metadata operations – modify object's metadata or perform metadata related
  adjustements on the object
* Row operations – Operations on whole rows such as filtering
* Field Operations – Derive fields, change fields, create fields,....
* Compositions – compose multiple objects into one
* Auditing – determine object properties of object's content
* Assertions – check whether object properties match certain conditions
* Conversions - convert objects to other types
* Output – generate output from objects

Currently implemented operations can be found at
http://www.scribd.com/doc/147247069/Bubbles-Brewery2-Operations.


Operation Context
=================

Operations are executed in an `operation context`. The context holds an
operation catalogue, decides which implementation is chosen for given objects
and decides execution policy.

Default context is provided as `default_context`.

Example:

.. code-block:: python

   from bubbles import default_context as c
   from bubbles import get_object

   source = get_object("csv", "data.csv")
   duplicates = c.op.duplicates(source)


To get operation catalogue, use: `context.debug_print_catalogue()`

Adding Custom Operations
========================

Example of `append` operation - append contents of objects sequentially.


.. note:: The `name` argument is not necessary if the functions are in
   different modules. In this case we have to name them differently, but
   provide equal operation name.

Version of the operation for list of iterators:

.. code-block:: python

    @operation("rows[]", name="append")
    def append_rows(ctx, objects):

        iterators = [iter(obj) for obj in objects]
        iterator = itertools.chain(*iterators)

        return IterableDataSource(iterator, objects[0].fields)

Version for list of SQL objects:

.. code-block:: python

    @operation("sql[]", name="append")
    def append_sql(ctx, objects):

        first = objects[0]

        statements = [o.sql_statement() for o in objects]
        statement = sqlalchemy.sql.expression.union(*statements)

        return first.clone_statement(statement=statement)

When we call `context.o.append(objects)` then appropriate version will be
chosen based on the objects and their representations. In this case all
objects have to have `sql` representation for the SQL version to be used.

Retry
-----

Sometimes it is not possible to preform composition, because the objects might
be from different databaseds. Or there might be some other reason why
operation might not be able to be performed on provided objects.

In this case the operation might give up, but still not fail – it might assume
that there might be some other operation that migh be able to complete desired
task. In our case, the SQL objects might not be composable:

.. code-block:: python

    @operation("sql[]", name="append")
    def append_sql(ctx, objects):
        first = objects[0]

        # Fail and use iterator version instead
        if not all(first.can_compose(o) for o in objects[1:]):
            raise RetryOperation(["rows", "rows[]"],
                                 reason="Can not compose")

        statements = [o.sql_statement() for o in objects]
        statement = sqlalchemy.sql.expression.union(*statements)

        return first.clone_statement(statement=statement)


