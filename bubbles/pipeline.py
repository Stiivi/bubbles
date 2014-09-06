# -*- coding: utf-8 -*-

from .graph import Graph, Node
from .errors import *

import copy

__all__ = [
            "Pipeline"
        ]


class Pipeline(object):
    def __init__(self, graph=None):
        """Creates a new pipeline.
        """
        self.graph = graph or Graph()
        self.labels = {}

        # Current node
        self.head = None

    def __or__(self, other):
        if isinstance(other, (NodeBase, Pipeline)):
            p = Pipeline(graph.copy())
            p.append_node(self)
            return p | other
        else:
            raise TypeError("Node can be piped only to another node or a "
                            "pipeline")

    def clone(self):
        """Creates a clone of the pipeline. The clone is a semi-shallow copy,
        with new graph and labels instances.  """

        clone = copy.deepcopy(self)
        return clone

    def __deepcopy__(self, memo):
        p = Pipeline(copy.deepcopy(graph, memo))
        p.labels = dict(self.labels)

        # Weak reference
        p.head = self.head

        return p

    def __getattr__(self, key):
        p = copy.copy(self)
        node = Node(key)

        if p.head:
            p.graph.connect(p.head, node)

        p.head = node

        return p

    def __call__(self, *args, **kwargs):
        # Configure node
        # TODO: handle joins
        self.head(*args, **kwargs)

        return self

    def label(self, name):
        """Assigns a label to the last node in the pipeline. This node can be
        later refereced as `pipeline[label]`. This method modifies the
        pipeline."""
        self.labels[name] = self.head
        return self

# TODO Obsolete
# =============

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

