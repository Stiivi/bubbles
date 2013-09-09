from collections import OrderedDict, namedtuple, Counter
from ..objects import data_object
from ..common import get_logger
from ..errors import *

__all__ = (
    "Graph",
    "Node",
    "Connection",

    # Not quite public
    "Node",
    "StoreObjectNode",
    "ObjectNode",
    "CreateObjectNode",
    "ObjectFactoryNode"
)

class NodeBase(object):
    def outlets(self, context):
        """Default node has no outlets."""
        return []

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
        # fixme: identify operands in *args
        op = context.operation(self.opname)
        args = list(operands) + list(self.args)
        result = op(*args, **self.kwargs)
        return result

    def __str__(self):
        return "operation %s" % self.opname

    def outlets(self, context):
        prototype = context.operation_prototype(self.opname)
        return prototype.operands


class ObjectFactoryNode(NodeBase):
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

class StoreObjectNode(NodeBase):
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
            raise ArgumentError("Unknown store '%s'" % self.store)

        return store.get_object(self.objname, **self.parameters)


    def __str__(self):
        return "soure %s in %s" % (self.objname, self.store)

class ObjectNode(NodeBase):
    def __init__(self, obj):
        self.obj = obj

    def is_source(self):
        return True

    def evaluate(self, engine, context, operands=None):
        """Returns the contained object."""
        return self.obj

    def __str__(self):
        return "object %s" % (self.obj, )


class CreateObjectNode(NodeBase):
    def __init__(self, store, name, *args, **kwargs):
        self.store = store
        self.name = name
        self.args = args
        self.kwargs = kwargs

    def is_source(self):
        return False

    def evaluate(self, engine, context, operands=None):
        if len(operands) != 1:
            raise ArgumentError("Number of operands for 'create object' should be 1")

        source = operands[0]

        try:
            store = engine.stores[self.store]
        except KeyError:
            raise ArgumentError("Unknown store %s" % self.store)

        target = store.create(self.name, source.fields,
                              *self.args, **self.kwargs)
        target.append_from(source)

        return target

    def outlets(self, context):
        """`Create` node has one outlet for an object that will be used to
        fill the created object's content."""
        return ["default"]

    def __str__(self):
        return("create %s in %s" % (self.name, self.store))


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


