# -*- coding: utf-8 -*-

from .context import default_context
from .engine import ExecutionEngine
from ..stores import open_store
from .graph import *
from ..errors import *
from ..dev import experimental

__all__ = [
            "Pipeline"
        ]

class Pipeline(object):
    def __init__(self, stores=None, context=None, graph=None, name=None):
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

        `name` is an optional user's pipeline identifier that is used for
        debugging purposes.

        .. note::

            You can set the `engine_class` variable to your own custom
            execution engine class with custom execution policy.

        """
        self.context = context or default_context

        self.stores = {}

        # List of owned and therefore opened stores
        self._owned_stores = []

        for name, store in stores.items():
            if isinstance(store, dict):
                store = dict(store)
                type_ = store.pop("type")
                store = open_store(type_, **store)
                self._owned_stores.append(store)

            self.stores[name] = store

        self.graph = graph or Graph()
        self.name = name

        # Set default execution engine
        self.engine_class = ExecutionEngine

        self.node = None

        self._test_if_needed = None
        self._test_if_satisfied = None

    def source(self, store, objname, **params):
        """Appends a source node to an empty pipeline. The source node will
        reference an object `objname` in store `store`. The actual object will
        be fetched during execution."""

        if self.node is not None:
            raise BubblesError("Can not set pipeline source: there is already "
                                "a node. Use new pipeline.")

        self.node = StoreObjectNode(store, objname, **params)
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
            node = ObjectFactoryNode(obj, **params)
        else:
            if params:
                raise ArgumentError("params should not be specified if "
                                    "object is provided.")
            node = ObjectNode(obj)

        self.graph.add(node)
        self.node = node

        return self

    def insert_into(self, store, objname, **params):
        """Appends a node that inserts into `objname` object in `store`.  The
        actual object will be fetched during execution."""

        if self.node is None:
            raise BubblesError("Cannot insert from a empty or disconnected "
                                "pipeline.")

        target = StoreObjectNode(store, objname, **params)
        self._append_insert_into(target)

        return self

    def insert_into_object(self, obj, **params):
        """If `obj` is a data object, then it is set as target for insert. If
        `obj` is a string, then it is considered as a factory and object is
        obtained using :fun:`data_object` with `params`"""

        if self.node is None:
            raise BubblesError("Cannot insert from a empty or disconnected "
                                "pipeline.")

        if isinstance(obj, str):
            node = ObjectFactoryNode(obj, **params)
        else:
            if params:
                raise ArgumentError("params should not be specified if "
                                    "object is provided.")
            node = ObjectNode(obj)

        self._append_insert_into(node)

        return self

    def _append_insert_into(self, target, **params):
        """Appends an `insert into` node."""

        insert = Node("insert")
        self.graph.add(insert)
        self.graph.add(target)
        self.graph.connect(target, insert, "target")
        self.graph.connect(self.node, insert, "source")

        self.node = insert

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
        it overrides the default context.

        There are two prerequisities for the pipeline to be run:

        * *test if needed* – pipeline is run only when needed, only when the
          test is satisfied. If the test is not satisfied
          (`ProbeAssertionError` is raised), the pipeline is gracefully
          skipped and considered successful without running.

        * *test if satisfied* - pipeline is run only when certain requirements
          are satisfied. If the requirements are not met, then an exception is
          raised and pipeline is not run.

        The difference between *"if needed"* and *"if satisfied"* is that the
        first one test whether the pipeline did already run or we already have
        the data. The second one *"if satisfied"* tests whether the pipeline
        will be able to run successfuly.

        """

        engine = self._get_engine(context)

        run = True
        if self._test_if_needed:
            try:
                engine.run(self._test_if_needed.graph)
            except ProbeAssertionError:
                name = self.name or "(unnamed)"
                self.context.logger.info("Skipping pipeline '%s', "
                                         "no need to run according to the test." %
                                         name)
                run = False

        if run and self._test_if_satisfied:
            try:
                engine.run(self._test_if_satisfied.graph)
            except ProbeAssertionError as e:
                name = self.name or "(unnamed)"
                reason = e.reason or "(unknown reason)"
                self.context.logger.error("Requirements for pipeline '%s' "
                            "are not satisfied. Reason: %s" % (name, reason))
                raise

        if run:
            result = engine.run(self.graph)
        else:
            result = None


        # TODO: run self._test_successful
        # TODO: run self.rollback if not sucessful

        return result

    def execution_plan(self, context=None):
        """Returns an execution plan of the pipeline as provided by the
        execution engine. For more information see
        :meth:`ExecutionEngine.execution_plan`.  """

        engine = self._get_engine(context)
        return engine.execution_plan(self.graph)

    def _get_engine(self, context=None):
        """Return a fresh engine instance that uses either target's context or
        explicitly specified other `context`."""
        context = context or self.context
        engine = self.engine_class(context=context, stores=self.stores)
        return engine

    def test_if_needed(self):
        """Create a branch that will be run before the pipeline is run. If the
        test passes, then pipeline will not be run. Use this, for example, to
        determine whether the data is already processed."""
        self._test_if_needed = Pipeline(self.stores, self.context)
        return self._test_if_needed

    def test_if_satisfied(self):
        """Create a branch that will be run before the pipeline is run. If the
        test fails, then pipeline will not be run and an error will be raised.
        Use this to test dependencies of the pipeline and avoid running an
        expensive process."""

        self._test_if_satisfied = Pipeline(self.stores, self.context)
        return self._test_if_satisfied

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

        operation = self.pipeline.context.operation(self.opname)

        if operation.opcount == 1:
            # Unary operation
            node = Node(self.opname, *args, **kwargs)
            self.pipeline._append_node(node)

        else:
            # n-ary operation. Take operands from arguments. There is one less
            # - the one that is just being created by this function as default
            # operand within the processing pipeline.

            operands = args[0:operation.opcount-1]
            args = args[operation.opcount-1:]

            # Get name of first (pipeline default) outlet and rest of the
            # operand outlets
            firstoutlet, *restoutlets = operation.operands

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

def create_pipeline(description):
    """Create a pipeline from a description. Description should be a list of
    operation descriptions. The operation is described as a tuple where first
    item is the operation name followed by operands. Last argument is a
    dictionary with options.

        ["operation", "operand", "operand", {foo}]
    """

    raise NotImplementedError

