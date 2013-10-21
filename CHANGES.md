Changes in Bubbles
==================

0.2
===

Overview
--------

* New data processing graph and new graph based `Pipeline` with customizable
  execution policy and with pre-execution tests
* New MongoDB backend with a store, data object and few demo ops
* New XLS backend with a store and data object
* New operations (see below)

Operations
----------

New operations:

* `filter_by_range`, `filter_not_empty`: rows, sql
* `split_date`: rows, sql
* `field_filter`: mongo (without `rename`)
* `distinct`: mongo
* `insert`: (rows, sql) and (sql, sql)
* `assert_contains`, `assert_missing`: sql
* `empty_to_missing`: rows – experimental
* `string_to_date`: rows – still experimental, format will change to SQL date
  format

Changed and fixed operations:

* `aggregate` accepts empty measure list – yields only count

New Features
------------

* Added `document` storage data type to represent JSON-like objects
* object store can be cloned using `clone()` which should provide another
  store with different configuration but possibility of mutually composable
  objects
* new `FieldError` exception
* Take into account object's data consumability on object use (naive
  implementation for the time being)
* CSVStore (`csv`) is now able to create CSV targets with `csv_target` factory
  name
* New `Resource` class representing file-like resources with optional call to
  `close()`
* Added `FileSystemStore` for read-only CSV and XLS files with default
  settings.
* Added `Store.exists()`, implemented in SQL backend.
* `ProbeAssertionError` has a `reason` attribute

Pipeline and execution:

* `Graph` and `Node` structure for building operation processing graphs
* operation list has an operation prototype that includes operation operand
  and parameter names
* Added `ExecutionEngine`, currently semi-private, but will serve as basis for
  future custom graph execution policies
* Added `Pipeline.execution_plan`
* Added thread_local - thread local variable storage
* Added `retry_deny` and `retry_allow` to the operation context
* Added insert operation accessible through `Pipeline.insert_into` and
  `Pipeline.insert_into_object`
* Added `test_if_needed()` and `test_if_satisfied()` methods which are fork()
  -like but executed before running the pipeline (see documentation for more
  information)


Changes
-------

* Original `Pipeline` implementation replaced – instead of immediate execution
  a graph is being created. Explicit `run()` is required.
* calling operations decorated with `@experimental` will cause a warning to be
  logged
* renamed module `doc` to `dev`, will contain more development tools in the
  future, such as operation auditing or data object API conformance checking
* `default_context` is now a thread-local variable, created on first use
* `open_resource` now returns a `Resource` object
* renamed engine `prepare_execution_plan` to `execution_plan`
* operation context's `o` accessor was renamed to `op` and now also supports
  getitem: `context.op["duplicates"]` is equal to `context.op.duplicates`.
* data objects should respond to `retained()` and `is_consumable()`
* default field storage type is now `string` instead of `unknown` for
  convenience.
* Removed default setting for debug logging, uses warning level
* Renamed namespace object name customization class variable `_ns_object_name`
  to `__identifier__`

Fixes
-----

* Problem described in the Issue #4 works as expected
* Fixed problem with filter_by_value
* Fixed aggregate key issues

