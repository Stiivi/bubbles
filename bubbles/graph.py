from collections import OrderedDict, namedtuple
from .objects import data_object
from .common import get_logger
from .errors import *
from .core import default_context

__all__ = (
    "Graph",
    "Node",
    "Connection",
    "ExecutionEngine",
    "Pipeline",

    # Not quite public
    "Node",
    "SourceNode",
    "ObjectNode",
    "FactorySourceNode"
)

class NodeBase(object):
    pass

class Node(NodeBase):
    def __init__(self, opname, *args, **kwargs):
        """Creates a `Node` with operation `op` and operation `options`"""

        self.opname = opname
        self.args = args
        self.kwargs = kwargs

    def is_source(self):
        return False

    def evaluate(self, engine, context, operands=None):
        """Evaluates the operation with name `opname` within `context`"""
        # FIXME: identify operands in *args
        op = context.operation(self.opname)
        args = list(operands) + list(self.args)
        result = op(*args, **self.kwargs)
        return result

    def __str__(self):
        return "operation %s" % self.opname

class FactorySourceNode(NodeBase):
    def __init__(self, factory, *args, **kwargs):

        self.factory = factory
        self.args = args
        self.kwargs = kwargs

    def is_source(self):
        return True

    def evaluate(self, engine, context, operands=None):
        return data_object(self.factory, *self.args, **self.kwargs)

    def __str__(self):
        return "factory source %s" % self.factory

class SourceNode(NodeBase):
    def __init__(self, store, objname, **parameters):
        self.store = store
        self.objname = objname
        self.parameters = parameters

    def is_source(self):
        return True

    def evaluate(self, engine, context, operands=None):
        """Looks up the object `objname` in `store` from `engine`."""

        try:
            store = engine.stores[self.store]
        except KeyError:
            raise ArgumentError("Unknown store %s" % store)

        return store.get_object(obj, **parameters)


    def __str__(self):
        return "soure %s in %s" % (objname, store)

class ObjectNode(NodeBase):
    def __init__(self, obj):
        self.obj = obj

    def is_source(self):
        return True

    def evaluate(self, engine, context, operands=None):
        """Returns the contained object."""
        return self.obj

Connection = namedtuple("Connection", ["source", "target", "outlet"])


class Graph(object):
    """Data processing graph.

    .. note:
            Modifications are not thread safe – as intended.
    """

    def __init__(self, nodes=None, connections=None):
        """Creates a node graph with connections.

        :Parameters:
            * `nodes` - dictionary with keys as node names and values as nodes
            * `connections` - list of two-item tuples. Each tuple contains source and target node
              or source and target node name.
        """

        super(Graph, self).__init__()
        self.nodes = OrderedDict()
        self.connections = set()

        self.logger = get_logger()

        self._name_sequence = 1

        if nodes:
            try:
                for name, node in nodes.items():
                    self.add(node, name)
            except:
                raise ValueError("Nodes should be a dictionary, is %s" % type(nodes))

        if connections:
            for connection in connections:
                self.connect(*connectio)

    def _generate_node_name(self):
        """Generates unique name for a node"""
        while 1:
            name = "node" + str(self._name_sequence)
            if name not in self.nodes.keys():
                break
            self._name_sequence += 1

        return name

    def add(self, node, name=None):
        """Add a `node` into the stream. Does not allow to add named node if
        node with given name already exists. Generate node name if not
        provided. Node name is generated as ``node`` + sequence number.
        Uniqueness is tested."""

        name = name or self._generate_node_name()

        if name in self.nodes:
            raise KeyError("Node with name %s already exists" % name)

        self.nodes[name] = node

        return name

    def node_name(self, node):
        """Returns name of `node`."""
        # There should not be more
        if not node:
            raise ValueError("No node provided")

        names = [key for key,value in self.nodes.items() if value==node]

        if len(names) == 1:
            return names[0]
        elif len(names) > 1:
            raise Exception("There are more references to the same node")
        else: # if len(names) == 0
            raise Exception("Can not find node '%s'" % node)

    def rename_node(self, node, name):
        """Sets a name for `node`. Raises an exception if the `node` is not
        part of the stream, if `name` is empty or there is already node with
        the same name. """

        if not name:
            raise ValueError("No node name provided for rename")
        if name in self.nodes():
            raise ValueError("Node with name '%s' already exists" % name)

        old_name = self.node_name(node)

        del self.nodes[old_name]
        self.nodes[name] = node

    def node(self, node):
        """Coalesce node reference: `reference` should be either a node name
        or a node. Returns the node object."""

        if isinstance(node, str):
            return self.nodes[node]
        elif node in self.nodes.values():
            return node
        else:
            raise ValueError("Unable to find node '%s'" % node)

    def remove(self, node):
        """Remove a `node` from the stream. Also all connections will be
        removed."""

        # Allow node name, get the real node object
        if isinstance(node, basestring):
            name = node
            node = self.nodes[name]
        else:
            name = self.node_name(node)

        del self.nodes[name]

        remove = [c for c in self.connections if c[0] == node or c[1] == node]

        for connection in remove:
            self.connections.remove(connection)

    def connect(self, source, target, outlet="default"):
        """Connects source node and target node. Nodes can be provided as
        objects or names."""
        # Get real nodes if names are provided
        source = self.node(source)
        target = self.node(target)

        sources = self.sources(target)
        if outlet in sources:
            raise GraphError("Target has already connection for outlet '%s'" % \
                                outlet)
        connection = Connection(source, target, outlet)
        self.connections.add(connection)

    def remove_connection(self, source, target):
        """Remove connection between source and target nodes, if exists."""

        connection = (self.coalesce_node(source), self.coalesce_node(target))
        self.connections.discard(connection)

    def sorted_nodes(self):
        """
        Returns topologically sorted nodes.

        Algorithm::

            L = Empty list that will contain the sorted elements
            S = Set of all nodes with no incoming edges
            while S is non-empty do
                remove a node n from S
                insert n into L
                for each node m with an edge e from n to m do
                    remove edge e from the graph
                    if m has no other incoming edges then
                        insert m into S
            if graph has edges then
                raise exception: graph has at least one cycle
            else
                return proposed topologically sorted order: L
        """
        def is_source(node, connections):
            for connection in connections:
                if node == connection.target:
                    return False
            return True

        def source_connections(node, connections):
            conns = set()
            for connection in connections:
                if node == connection.source:
                    conns.add(connection)
            return conns

        nodes = set(self.nodes.values())
        connections = self.connections.copy()
        sorted_nodes = []

        # Find source nodes:
        source_nodes = set([n for n in nodes if is_source(n, connections)])

        # while S is non-empty do
        while source_nodes:
            # remove a node n from S
            node = source_nodes.pop()
            # insert n into L
            sorted_nodes.append(node)

            # for each node m with an edge e from n to m do
            s_connections = source_connections(node, connections)
            for connection in s_connections:
                #     remove edge e from the graph
                m = connection.target
                connections.remove(connection)
                #     if m has no other incoming edges then
                #         insert m into S
                if is_source(m, connections):
                    source_nodes.add(m)

        # if graph has edges then
        #     output error message (graph has at least one cycle)
        # else
        #     output message (proposed topologically sorted order: L)

        if connections:
            raise Exception("Steram has at least one cycle (%d connections left of %d)" % (len(connections), len(self.connections)))

        return sorted_nodes

    def targets(self, node):
        """Return nodes that `node` passes data into."""
        node = self.node(node)
        nodes =[conn.target for conn in self.connections if conn.target == node]
        return nodes

    def sources(self, node):
        """Return a dictionary where keys are outlet names and values are
        nodes."""
        node = self.node(node)

        nodes = {}
        for conn in self.connections:
            if conn.target == node:
                nodes[conn.outlet] = conn.source

        return nodes


class ExecutionStep(object):
    def __init__(self, node, outlets=None, result=None):
        self.node = node
        self.outlets = outlets
        self._result = result
        self._used = False

    def evaluate(self, engine, context, operands):
        """Evaluates the wrapped node within `context` and with `operands`.
        Stores the evaluation result."""

        self.result = self.node.evaluate(engine, context, operands)
        return self.result

    def __str__(self):
        return "evaluate %s" % str(self.node)

    @property
    def result(self):
        if self._used and self._result.is_consumable():
            raise BubblesError("Consumable result %s used twice" % self._result)

        self._used = True
        return self._result

    @result.setter
    def result(self, value):
        self._result = value

# Execution Engine
# ================
#
# Main graph execution class. Current execution method is simple:
# 1. topologically sort nodes
# 2. prepare execution node for each node and connects them by outlets
# 3. execute in the topological order and set result to the execution node
#    – result is used as input for other nodes

# TODO: allow use of lists of objects, such as rows[] or sql[]. Currently
# there is no way how to specify this kind of connections in the graph.

class ExecutionEngine(object):

    def __init__(self, context, stores=None):
        """Creates an instance of execution engine within an execution
        `context`.

        `stores` is a mapping of store names and opened data stores. Stores
        are used when resoving data sources by reference."""

        self.stores = stores or {}
        self.context = context
        self.logger = context.logger

    def outlets_for_node(self, node):
        """Return list of outlet names for `node`. Outlet names are derived
        from a prototype of nodes's operation within execution context."""

        # Consider source node
        if node.is_source():
            return []

        prototype = self.context.operation_prototype(node.opname)
        return prototype.operands

    def prepare_execution_plan(self, graph):
        """Returns a list of topologically sorted `ExecutionSteps`, ready to
        be used for execution.

        If node is an operation node, it will contain references to nodes
        holding results that will be passed as operands to the operation.

        """

        # TODO: this method will be customizable in subclasses in the future

        # Operation -> Node -> Execution Step
        # Node is an operation with parameters set (configured operation)
        # Execution Node is Node in execution context with bound outlets

        sorted_nodes = graph.sorted_nodes()

        node_steps = {}
        plan = []

        for node in sorted_nodes:
            sources = graph.sources(node)
            outlets = self.outlets_for_node(node)

            outlet_nodes = []
            for i, outlet in enumerate(outlets):
                if i == 0:
                    outlet_node = sources.get(outlet) or sources.get("default")
                else:
                    outlet_node = sources.get(outlet)

                if not outlet_node:
                    raise BubblesError("Outlet '%s' is not connected" %
                            outlet)

                # Get execution node wrapper for the outlet node
                outlet_nodes.append(node_steps[outlet_node])

            step = ExecutionStep(node, outlets=outlet_nodes)

            node_steps[node] = step
            plan.append(step)

        return plan

    def run(self, graph):
        """Runs the `graph` nodes. First an execution plan is prepared, then
        the nodes are executed according to the plan. See
        :meth:`ExecutionEngine.prepare_execution_plan` for more information."""

        plan = self.prepare_execution_plan(graph)

        for i, step in enumerate(plan):
            self.logger.debug("step %s: %s" % (i, str(step)))

            if step.outlets:
                operands = [o.result for o in step.outlets]
            else:
                operands = []

            step.evaluate(self, self.context, operands)

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
        """
        self.context = context or default_context
        self.stores = stores or {}
        self.graph = graph or Graph()

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

    def __getattr__(self, name):
        """Adds operation node into the stream"""

        return _PipelineOperation(self, name)

    def run(self, context=None):
        """Runs the pipeline in Pipeline's context. If `context` is provided
        it overrides the default context."""

        context = context or self.context
        engine = ExecutionEngine(context=context, stores=self.stores)
        engine.run(self.graph)

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

