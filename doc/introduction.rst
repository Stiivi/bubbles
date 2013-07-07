++++++++++++
Introduction
++++++++++++

Bubbles is a python framework for data processing and data quality
measurement. Basic concept are abstract data objects, operations and dynamic
operation dispatch.

.. raw:: html

   <iframe src="http://www.slideshare.net/slideshow/embed_code/22475745" width="427" height="356" frameborder="0" marginwidth="0" marginheight="0" scrolling="no" style="border:1px solid #CCC;border-width:1px 1px 0;margin-bottom:5px" allowfullscreen webkitallowfullscreen mozallowfullscreen> </iframe> <div style="margin-bottom:5px"> <strong> <a href="http://www.slideshare.net/Stiivi/data-brewery-2-data-objects" title="Bubbles – Virtual Data Objects" target="_blank">Bubbles – Virtual Data Objects</a> </strong> from <strong><a href="http://www.slideshare.net/Stiivi" target="_blank">Stefan Urbanek</a></strong> </div>

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

When you might consider using bubbles?

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

