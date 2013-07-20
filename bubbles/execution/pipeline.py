# -*- coding: utf-8 -*-

from .context import default_context
from .engine import ExecutionEngine
from .graph import *
from ..errors import *
from ..dev import experimental

__all__ = [
            "Pipeline"
        ]

class Pipeline(object):
    def __init__(self, stores=None, context=None, graph=None):
        """Creates a new pipeline with `context` and sets current object to
        `obj`. If no context is provided, default one is used.

        Pipeline inherits operations from the `context` and uses context's
        dispatcher to call the operations. Operations are provided as
        pipeline's methods:

        .. code-block:: python

            p = Pipeline(stores={"default":source_store})
            p.source("default", "data")
            # Call an operation within context
            p.distinct("city")

            p.create("default", "cities")
            p.run()


        .. note::

            You can set the `engine_class` variable to your own custom
            execution engine class with custom execution policy.

        """
        self.context = context or default_context
        self.stores = stores or {}
        self.graph = graph or Graph()

        # Set default execution engine
        self.engine_class = ExecutionEngine

        self.node = None

    def source(self, store, objname, **params):
        """Appends a source node to an empty pipeline. The source node will
        reference an object `objname` in store `store`. The actual object will
        be fetched during execution."""

        if self.node is not None:
            raise BubblesError("Can not set pipeline source: there is already "
                                "a node. Use new pipeline.")

        self.node = SourceNode(store, objname, **params)
        self.graph.add(self.node)

        return self

    def source_object(self, obj, **params):
        """If `obj` is a data object, then it is set as source. If `obj` is a
        string, then it is considered as a factory and object is obtained
        using :fun:`data_object` with `params`"""
        if self.node is not None:
            raise BubblesError("Can not set pipeline source: there is already "
                                "a node. Use new pipeline.")

        if isinstance(obj, str):
            node = FactorySourceNode(obj, **params)
        else:
            if params:
                raise ArgumentError("params should not be specified if "
                                    "object is provided.")
            node = ObjectNode(obj)

        self.graph.add(node)
        self.node = node

        return self

    def create(self, store, name, *args, **kwargs):
        """Create new object `name` in store `name`. """

        node = CreateObjectNode(store, name, *args, **kwargs)
        self._append_node(node)

        return self

    def __getattr__(self, name):
        """Adds operation node into the stream"""

        return _PipelineOperation(self, name)

    def fork(self, empty=None):
        """Forks the current pipeline. If `empty` is ``True`` then the forked
        fork has no node set and might be used as source."""
        fork = Pipeline(self.stores, self.context, self.graph)

        # TODO: make the node non-consumable
        if not empty:
            fork.node = self.node

        return fork

    def _append_node(self, node, outlet="default"):
        """Appends a node to the pipeline stream. The new node becomes actual
        node."""

        node_id = self.graph.add(node)
        if self.node:
            self.graph.connect(self.node, node, outlet)
        self.node = node

        return self

    def run(self, context=None):
        """Runs the pipeline in Pipeline's context. If `context` is provided
        it overrides the default context."""

        context = context or self.context
        engine = self.engine_class(context=context, stores=self.stores)
        engine.run(self.graph)


class _PipelineOperation(object):
    def __init__(self, pipeline, opname):
        """Creates a temporary object that will append an operation to the
        pipeline"""
        self.pipeline = pipeline
        self.opname = opname

    def __call__(self, *args, **kwargs):
        """Appends an operation (previously stated) with called arguments as
        operarion's parameters"""
        # FIXME: works only with unary operations, otherwise undefined
        # TODO: make this work with binary+ operations
        #
        #
        # Dev note:
        # – `operands` might be either a node - received throught
        # pipeline.node or it might be a pipeline forked from the receiver

        prototype = self.pipeline.context.operation_prototype(self.opname)

        if prototype.operand_count == 1:
            # Unary operation
            node = Node(self.opname, *args, **kwargs)
            self.pipeline._append_node(node)

        else:
            # n-ary operation. Take operands from arguments. There is one less
            # - the one that is just being created by this function as default
            # operand within the processing pipeline.

            operands = args[0:prototype.operand_count-1]
            args = args[prototype.operand_count-1:]

            # Get name of first (pipeline default) outlet and rest of the
            # operand outlets
            firstoutlet, *restoutlets = prototype.operands

            # Pipeline node - default
            node = Node(self.opname, *args, **kwargs)
            self.pipeline._append_node(node, firstoutlet)

            # All operands should be pipeline objects with a node
            if not all(isinstance(o, Pipeline) for o in operands):
                raise BubblesError("All operands should come from a Pipeline")

            # Operands are expected to be pipelines forked from this one
            for outlet, operand in zip(restoutlets, operands):
                # Take current node of the "operand pipeline"
                src_node = operand.node

                # ... and connect it to the outlet of the currently created
                # node
                self.pipeline.graph.connect(src_node, node, outlet)

        return self.pipeline

