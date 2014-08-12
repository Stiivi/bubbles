########
Overview
########

Adding New Operations
=====================

Custom Kernel

Merging Kernels

Customizing Operation 
=====================

.. code-block:: python

    @operation("rows", name="pretty_print")
    def no_pretty(obj):
        print("No pretty!")

    k.remove_operation("pretty_print", ["rows"])
    k.register_operation(no_pretty)

    k.pretty_print(obj)

