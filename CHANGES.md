Changes in Bubbles
==================

Current Version
===============

Overview
--------

* New data processing graph and new graph based `Pipeline`
* Graph execution policy is separate and might be customized in the future
* Take into account object's data consumability on object use (naive
  implementation for the time being)
* New operations (see below)

Operation Changes
-----------------

New:

* `filter_by_range`, `filter_not_empty`: rows, sql
* `split_date`: rows, sql
* `string_to_date`: rows – still experimental, format will change to SQL date
  format

New Features
------------

* new `FieldError` exception
* object store can be cloned using `clone()` which should provide another
  store with different configuration but possibility of mutually composable
  objects
* CSVStore (`csv`) is now able to create CSV targets
* `Graph` and `Node` structure for building operation processing graphs
* New `Pipeline` class for simplified building of `Graph`
* data objects should respond to `retained()` and `is_consumable()`
* operation list has an operation prototype that includes operation operand
  and parameter names
* Added `ExecutionEngine`, currently semi-private, but will serve as basis for
  future custom graph execution policies

Changes
-------

* calling operations decorated with `@experimental` will cause a warning to be
  logged
* renamed module `doc` to `dev`, will contain more development tools in the
  future, such as operation auditing or data object API conformance checking
* Original `Pipeline` renamed to `ImmediatePipeline`

Fixes
-----

None so far.

