# -*- coding: utf-8 -*-
from collections import namedtuple, Counter
from .op import get_registry
from .context import Context
from .common import get_logger
from .errors import *

__all__ = (
    "ExecutionEngine",

    # TODO: Not quite public yet, but we export it nonetheless
    "ExecutionStep",
    "ExecutionPlan"
)


class ExecutionStep(object):
    def __init__(self, node, outlets=None, result=None):
        self.node = node
        self.outlets = outlets or []
        self.result = result

    def evaluate(self, engine, context, operands):
        """Evaluates the wrapped node within `context` and with `operands`.
        Stores the evaluation result."""

        self.result = self.node.evaluate(engine, context, operands)
        return self.result

    def __str__(self):
        return "evaluate %s" % str(self.node)


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

ExecutionPlan = namedtuple("ExecutionPlan", ["steps", "consumption"])

# Execution Engine
#
# This is the core graph execution class. In the future it will contain more
# helper methods for node execution.
#
# Sublcasses should implement the prepare_execution_plan method that returns a
# tuple ExecutionPlan(steps, consumption) where steps is a list of
# ExecutionSteps and consumption is a dictionary mapping a node to number of
# times the node output is consumed within the graph.

class ExecutionEngine(object):

    def __init__(self, context=None, stores=None):
        """Creates an instance of execution engine within an execution
        `context`.

        `stores` is a mapping of store names and opened data stores. Stores
        are used when resoving data sources by reference.

        Execution engine is also used in :class:`Pipeline` objects to run the
        pipelines.
        """

        self.stores = stores or {}
        if context:
            self.context = context
        else:
            self.context = Context()
            # Register default operations
            self.context.register_operations(get_registry().values())

        self.logger = get_logger()

    def execution_plan(self, graph):
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
        steps = []

        # Count consumption of node's output and add consumption hints to the
        # execution plan. ExecutionEngine should handle (or refuse to handle)
        # multiple consumptions of consumable objects
        consumption = Counter()

        for node in sorted_nodes:
            sources = graph.sources(node)
            outlets = node.outlets(self.context)

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

                # Count the consumption (see note before the outer loop)
                consumption[outlet_node] += 1

            step = ExecutionStep(node, outlets=outlet_nodes)

            node_steps[node] = step
            steps.append(step)

        plan = ExecutionPlan(steps, consumption)

        return plan

    def run(self, graph):
        """Runs the `graph` nodes. First an execution plan is prepared, then
        the nodes are executed according to the plan. See
        :meth:`ExecutionEngine.prepare_execution_plan` for more information.
        """

        # TODO: write documentation about consumable objects

        # FIXME: FROM HERE vvvvvvv
        for name, store in self.stores.items():
            if isinstance(store, dict):
                store = dict(store)
                type_ = store.pop("type")
                store = open_store(type_, **store)
                self._owned_stores.append(store)

            self.stores[name] = store

        plan = self.execution_plan(graph)
        # FIXME: TO HERE ^^^^^^^

        # Set of already consumed nodes
        consumed = set()

        for i, step in enumerate(plan.steps):
            self.logger.debug("step %s: %s" % (i, str(step)))

            operands = []

            for outlet in step.outlets:

                # Check how many times the outlet node that is about to be
                # used is going to be consumed. If it is consumable and will
                # be consumed more than once, then a retained version of the
                # object is created. Retention policy is defined by the
                # backend. In most of the cases it is just python list wrapper
                # over consumed iterator of rows, which might be quite costly.

                consume_times = plan.consumption[outlet.node]
                if outlet.result.is_consumable() and consume_times > 1:
                    if outlet.node not in consumed:
                        self.logger.debug("retaining consumable %s. it will "
                                          "be consumed %s times" % \
                                                 (outlet.node, consume_times))
                        outlet.result = outlet.result.retained()

                consumed.add(outlet.node)
                operands.append(outlet.result)

            step.evaluate(self, self.context, operands)
