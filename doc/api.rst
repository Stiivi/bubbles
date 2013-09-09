###
API
###


Core
====

Metadata
--------

.. autoclass:: bubbles.Field

.. autoclass:: bubbles.FieldList

.. autoclass:: bubbles.FieldFilter


Objects
-------

.. autofunction:: bubbles.data_object

.. autoclass:: bubbles.DataObject

.. autofunction:: bubbles.shared_representations

Stores
------

.. autofunction:: bubbles.open_store

.. autofunction:: bubbles.copy_object

.. autoclass:: bubbles.DataStore


Context and Execution
---------------------

.. autoclass:: bubbles.OperationContext

.. autoclass:: bubbles.Signature

.. autofunction:: bubbles.operation

.. autoclass:: bubbles.Operation
   
.. autoclass:: bubbles.Graph

.. autoclass:: bubbles.LoggingContextObserver

.. autoclass:: bubbles.CollectingContextObserver

Pipeline
--------

.. autoclass:: bubbles.Pipeline

Various utilities
-----------------

.. autofunction:: bubbles.guess_type

.. autofunction:: bubbles.expand_record

.. autofunction:: bubbles.collapse_record

.. autofunction:: bubbles.open_resource

.. autoclass:: bubbles.prepare_key

.. autoclass:: bubbles.prepare_aggregation_list

.. autoclass:: bubbles.prepare_order_list

Internals
=========

.. autofunction:: get_logger

