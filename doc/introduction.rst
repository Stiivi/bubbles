++++++++++++
Introduction
++++++++++++

Bubbles is a python framework for data processing and data quality
measurement. Basic concept are abstract data objects, operations and dynamic
operation dispatch.

Priorities of the framework are:

* understandability of the process
* auditability of the data being processed (frequent use of metadata)
* usability
* versatility

Bubbles is performance agnostic at the low level of physical data
implementation. Performance should be assured by the data technology and
proper use of operations.

Uses
====

When you might consider using brewery?

* data integration
* data cleansing
* data monitoring
* data auditing
* learn more about unknown datasets
* heterogenous data environments – different data technologies

Modules
=======

The framework consists of several logical modules (not published as Python
modules):

* `metadata` – field types and field type operations, describe structure of
  data

* `objects` – data object core
* `stores` – stores of data objects
* `core` – operation core, includes OperationContext
* `backends` – various backends such as SQL or text (CSV)

