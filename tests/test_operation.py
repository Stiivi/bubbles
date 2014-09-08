import unittest

from bubbles.operation import Operation, get_signature, Signature, Any
from bubbles.errors import ArgumentError


def empty():
    pass

def source(context):
    pass

def unary(context, left):
    pass

def invalid_unary(context, left:None):
    pass

def binary(context, left, right:"sql"):
    pass

def generator(context, count=10):
    for i in range(0, count):
        yield [i]

class SignatureTestCase(unittest.TestCase):
    def setUp(self):
        pass

    def test_get_signature(self):
        with self.assertRaises(ArgumentError):
            sig = get_signature(empty)

        with self.assertRaises(ArgumentError):
            sig = get_signature(invalid_unary)

        sig = get_signature(source)
        self.assertEqual(len(sig), 0)

        sig = get_signature(unary)
        self.assertEqual(len(sig), 1)

        sig = get_signature(binary)
        self.assertEqual(len(sig), 2)

        sig = get_signature(generator)
        self.assertEqual(len(sig), 0)

    def test_signatures_operators(self):
        s1 = Signature("x", "x", "x")
        s2 = Signature("x", "x", "x")
        s3 = Signature("x", Any, "x")
        s4 = Signature(Any, "x", "x")
        s5 = Signature(Any, Any, "x")

        self.assertTrue(s1 == s2)
        self.assertFalse(s1 != s2)
        self.assertTrue(s1 != s3)

        self.assertTrue(s1 > s3)
        self.assertTrue(s1 > s4)
        self.assertTrue(s1 > s5)

        self.assertTrue(s3 < s1)
        self.assertTrue(s4 < s1)
        self.assertTrue(s5 < s1)

    def test_sorted_signatures(self):
        s1 = Signature("x", "x", "x")
        s2 = Signature("x", Any, "x")
        s3 = Signature(Any, "x", "x")
        s4 = Signature(Any, Any, "x")
        s5 = Signature(Any, Any, Any)

        signatures = [s2, s3, s1, s5, s4]
        sigsorted = [s1, s2, s3, s4, s5]

        self.assertCountEqual(sorted(signatures), sigsorted)

    def test_match(self):
        self.assertTrue(Signature("sql").match("sql"))
        self.assertTrue(Signature("*").match("sql"))
        self.assertTrue(Signature("sql[]").match("sql[]"))
        self.assertTrue(Signature("*[]").match("sql[]"))

        self.assertFalse(Signature("sql").match("rows"))
        self.assertFalse(Signature("sql").match("sql[]"))

class OperationTestCase(unittest.TestCase):
    def setUp(self):
        pass

    def test_basic(self):
        node = Operation("generator")
        node.register(generator)
        self.assertEqual(node.name, "generator")
        self.assertEqual(len(node.registry), 1)
        self.assertEqual(len(node.prototype), 0)

        node = Operation("unary")
        node.register(unary)
        self.assertEqual(node.name, "unary")
        self.assertEqual(len(node.registry), 1)
        self.assertEqual(len(node.prototype), 1)

    def test_incompatible_prototype(self):
        node = Operation("unary")
        node.register(unary)
        with self.assertRaises(ArgumentError):
            node.register(binary)

    def test_nodes(self):

        orders = op.source("customers").filter()
        customers = op.source("customers")
        result = orders.join(customers)

