import unittest
from bubbles.threadlocal import LocalProxy, thread_locals

class TestLocalProxy(unittest.TestCase):
    def setUp(self):
        try:
            del thread_locals.test
        except AttributeError:
            pass

    def test_no_local(self):

        proxy = LocalProxy("test")
        with self.assertRaises(RuntimeError):
            proxy["foo"] = "bar"

    def test_with_local(self):
        thread_locals.test = {}
        proxy = LocalProxy("test")
        proxy["foo"] = "bar"

        self.assertEqual("bar", thread_locals.test["foo"])

    def test_factory(self):
        proxy = LocalProxy("test", factory=dict)
        self.assertTrue(isinstance(proxy._represented_local_object(), dict))
        self.assertEqual(0, len(proxy))

        proxy["foo"] = "bar"
        self.assertEqual("bar", thread_locals.test["foo"])
