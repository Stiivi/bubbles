import unittest
from bubbles import Graph, Node, GraphError

class GraphTestCase(unittest.TestCase):
    def test_basic(self):
        g = Graph()
        g.add(Node("src"), "n1")
        g.add(Node("distinct"),"n2")
        g.add(Node("pretty_print"), "n3")

        self.assertEqual(3, len(g.nodes))

        g.connect("n1", "n2")

        sources = g.sources("n2")
        self.assertEqual(1, len(sources))
        self.assertTrue(isinstance(sources["default"], Node))
        self.assertEqual("src", sources["default"].opname)

    def test_ports(self):
        g = Graph()
        g.add(Node("dim"), "dim")
        g.add(Node("src"), "src")
        g.add(Node("join_detail"), "j")

        g.connect("dim", "j", "master")

        with self.assertRaises(GraphError):
            g.connect("src", "j", "master")

        g.connect("src", "j", "detail")

        sources = g.sources("j")
        self.assertEqual(2, len(sources))
        self.assertEqual(["detail", "master"], sorted(sources.keys()))


if __name__ == "__main__":
    unittest.main()
