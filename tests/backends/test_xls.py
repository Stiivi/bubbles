import unittest
from ..common import data_path

from bubbles import *
from bubbles.backends.xls import XLSStore, XLSObject

class XLSBackendTestCase(unittest.TestCase):
    def test_load(self):
        obj = XLSObject(data_path("data.xls"), "amounts", encoding="latin1")
        self.assertEqual(["id", "name", "amount"], obj.fields.names())

        rows = list(obj.rows())
        self.assertEqual(4, len(rows))
        self.assertEqual(4, len(obj))

        self.assertSequenceEqual([1, "Adam", 10], rows[0])

    def test_skip(self):
        obj = XLSObject(data_path("data.xls"), "numbers", skip_rows=2)
        self.assertEqual(["number", "name"], obj.fields.names())

        rows = list(obj.rows())
        self.assertEqual(10, len(rows))
        self.assertEqual(10, len(obj))

        self.assertSequenceEqual([1, "one"], rows[0])

    def test_skip_too_much(self):
        with self.assertRaises(ArgumentError):
            obj = XLSObject(data_path("data.xls"), "numbers", skip_rows=20)

    def test_store(self):
        store = XLSStore(data_path("data.xls"))
        self.assertSequenceEqual(["amounts", "numbers"], store.object_names())

        obj = store.get_object("numbers", skip_rows=2)
        self.assertEqual(10, len(obj))

if __name__ == "__main__":
    unittest.main()
