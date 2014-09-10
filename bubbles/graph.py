from collections import OrderedDict, namedtuple, Counter
from .objects import data_object
from .errors import *

__all__ = (
    "Graph",
    "Node",
    "Connection",
)

class Node(object):
    def __init__(self, op, *args, **kwargs):
        """Creates a `Node` with operation `op` and operation `options`"""

        self.opname = op

        self.args = None
        self.kwargs = None

        self.configure(*args, **kwargs)

    def configure(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __str__(self):
        return "<{} {}>".format(self.opname, id(self))

    def __repr__(self):
        return "Node({})".format(self.opname)


Connection = namedtuple("Connection", ["source", "target", "outlet"])

class Graph(object):
    """Data processing graph.

    .. note:
            Modifications are not thread safe – as intended.
    """

    def __init__(self):
        """Creates a node graph with connections.

        :Parameters:
            * `nodes` - dictionary with keys as node names and values as nodes
            * `connections` - list of two-item tuples. Each tuple contains source and target node
              or source and target node name.
        """

        super(Graph, self).__init__()
        self.nodes = set()
        self.connections = set()
        self.labels = {}

    def add(self, node, name=None):
        """Add a `node` into the stream. Does not allow to add named node if
        node with given name already exists. Generate node name if not
        provided. Node name is generated as ``node`` + sequence number.
        Uniqueness is tested."""

        self.nodes.add(node)
        if name:
            self.labels[name] = node

    def update(self, other):
        self.nodes |= other.nodes
        self.connections |= other.connections

    def node(self, node):
        """Coalesce node reference: `reference` should be either a node name
        or a node. Returns the node object."""

        if isinstance(node, str):
            return self.labels[node]
        elif node in self.nodes:
            return node
        else:
            raise ValueError("Unable to find node '%s'" % node)

    def remove(self, node):
        """Remove a `node` from the stream. Also all connections will be
        removed."""

        # Allow node name, get the real node object
        self.nodes.remove(node)

        for key, value in self.labels.items():
            if value is node:
                del self.labels[key]
                break

        remove = [c for c in self.connections if c[0] is node or c[1] is node]

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

        connections = self.connections.copy()
        sorted_nodes = []

        # Find source nodes:
        source_nodes = set([n for n in self.nodes if is_source(n, connections)])

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


