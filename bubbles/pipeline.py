# -*- coding: utf-8 -*-

from .graph import Graph, Node
from .errors import PipelineError
from collections import OrderedDict

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

    def copy(self):
        """Creates a clone of the pipeline. The clone is a semi-shallow copy,
        with new graph and labels instances.  """

        p = Pipeline()
        p.graph.update(self.graph)
        p.labels = dict(self.labels)
        p.head = self.head

        return p

    def __getattr__(self, key):
        return _PipelineHead(self, key)


    def label(self, name):
        """Assigns a label to the last node in the pipeline. This node can be
        later refereced as `pipeline[label]`. This method modifies the
        pipeline."""
        self.labels[name] = self.head
        return self


class _PipelineHead(object):
    def __init__(self, pipeline, node_name):
        self.pipeline = pipeline
        self.node_name = node_name

    def __call__(self, *args, **kwargs):
        p = self.pipeline.copy()

        node = Node(self.node_name)
        p.graph.add(node)

        if p.head:
            # import pdb; pdb.set_trace()
            p.graph.connect(p.head, node)

        p.head = node
        p.head.configure(*args, **kwargs)


        # Gather input outlets
        # 1. gather anonymous outlets – appearance position in the argument
        #    list is the outlet number
        # 2. gather named outlets – key-word argument which is a node is
        #    considered an outlet

        # TODO: make this a "pipeline reference"
        operands = []
        for key, value in enumerate(args):
            if isinstance(value, Pipeline):
                operands.append((key, value))

        for key, value in kwargs.items():
            if isinstance(value, Pipeline):
                operands.append((key, value))

        names = []
        for name, other in operands:
            names.append(name)

            if other is not self.pipeline:
                p.graph.update(other.graph)

            print("--- connecting ops: %s -> %s as %s" %
                       (other.head, p.head, name))
            p.graph.connect(other.head, p.head, name)

        p.head.operands = names

        return p

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
                raise PipelineError("All operands should come from a Pipeline")

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

