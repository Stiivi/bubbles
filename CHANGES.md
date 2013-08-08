Changes in Bubbles
==================

Current Version
===============

Overview
--------

* New MongoDB backend with store, object and ops
* New XLS backend with store and object

* New data processing graph and new graph based `Pipeline`
* Graph execution policy is separate and might be customized in the future
* New operations (see below)

Operation Changes
-----------------

New:

* `filter_by_range`, `filter_not_empty`: rows, sql
* `split_date`: rows, sql
* `string_to_date`: rows – still experimental, format will change to SQL date
  format
* `field_filter`: mongo
* `distinct`: mongo

Changes and fixes:

* `aggregate` accepts empty measure list – yields only count

New Features
------------

* new `FieldError` exception
* object store can be cloned using `clone()` which should provide another
  store with different configuration but possibility of mutually composable
  objects
* Take into account object's data consumability on object use (naive
  implementation for the time being)
* CSVStore (`csv`) is now able to create CSV targets
* `Graph` and `Node` structure for building operation processing graphs
* New `Pipeline` class for simplified building of `Graph`
* data objects should respond to `retained()` and `is_consumable()`
* operation list has an operation prototype that includes operation operand
  and parameter names
* Added `ExecutionEngine`, currently semi-private, but will serve as basis for
  future custom graph execution policies
* Added `document` storage data type to represent JSON-like objects
* Added thread_local - thread local variable storage
* New `Resource` class representing file-like resources with optional call to
  `close()`
* Added `Pipeline.execution_plan`

Changes
-------

* calling operations decorated with `@experimental` will cause a warning to be
  logged
* renamed module `doc` to `dev`, will contain more development tools in the
  future, such as operation auditing or data object API conformance checking
* Original `Pipeline` implementation removed 
* `default_context` is now a thread-local variable, created on first use
* `open_resource` now returns a `Resource` object
* renamed engine `prepare_execution_plan` to `execution_plan`

Fixes
-----

* Problem described in the Issue #4 works as expected
* Fixed problem with filter_by_value
* Fixed aggregate key issues

