import unittest
from bubbles import Pipeline, Graph, Node, GraphError

class PipelineTestCase(unittest.TestCase):
    def test_basic(self):
        p = Pipeline()
        self.assertIsInstance(p.graph, Graph)
        self.assertEqual(len(p.graph.nodes), 0)

    def test_create_head(self):
        p = Pipeline()

        p2 = p.some_operation()

        # Make sure that we get different copy
        self.assertIsNot(p, p2)
        self.assertIsNot(p.graph, p2.graph)
        self.assertNotEqual(p.graph.nodes, p2.graph.nodes)

        self.assertEqual(len(p2.graph.nodes), 1)

        node = list(p2.graph.nodes)[0]
        self.assertIsInstance(node, Node)
        self.assertEqual(node.opname, "some_operation")

    def test_chain(self):
        p = Pipeline()
        p = p.one().two().three()

        self.assertEqual(len(p.graph.nodes), 3)
        self.assertEqual(len(p.graph.connections), 2)

    def test_head_node_properties(self):
        p = Pipeline().some_operation(foo=10, bar="baz")

        node = list(p.graph.nodes)[0]
        self.assertCountEqual(node.kwargs.keys(), ["foo", "bar"])
        self.assertCountEqual(node.kwargs.values(), [10, "baz"])

    def test_head_node_properties(self):
        # A - B - C --\
        #     \------ D

        common = Pipeline().node_A().node_B()
        p = common.node_C()

        self.assertEqual(len(p.graph.nodes), 3)
        self.assertEqual(len(p.graph.connections), 2)

        p = p.node_D(common)

        self.assertEqual(len(p.graph.nodes), 4)
        names = [n.opname for n in p.graph.nodes]
        self.assertCountEqual(names, ["node_A", "node_B", "node_C", "node_D"])

        self.assertCountEqual(p.head.operands, [0])

        self.assertEqual(len(p.graph.connections), 4)

        connections = []
        for con in p.graph.connections:
            connections.append([con.source.opname,
                                con.target.opname,
                                con.outlet])

        self.assertCountEqual(connections,
                              [
                                  ["node_A", "node_B", "default"],
                                  ["node_B", "node_C", "default"],
                                  ["node_C", "node_D", "default"],
                                  ["node_B", "node_D", 0],
                              ])


