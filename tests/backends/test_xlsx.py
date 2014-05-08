import unittest
from ..common import data_path

from bubbles.backends.xlsx import XLSXStore, XLSXObject
from bubbles.metadata import FieldList
from bubbles.errors import ArgumentError
from bubbles.stores import open_store


class XLSXBackendTestCase(unittest.TestCase):
    def test_load(self):
        obj = XLSXObject(data_path("data.xlsx"), encoding="latin1")
        self.assertEqual(["id", "name", "amount"], obj.fields.names())

        rows = list(obj.rows())
        self.assertEqual(4, len(rows))
        self.assertEqual(4, len(obj))

        self.assertSequenceEqual([1, "Adam", 10], rows[0])

    def test_skip(self):
        obj = XLSXObject(data_path("data.xlsx"), FieldList("number", "name"),
                        skip_rows=2)
        self.assertEqual(["number", "name"], obj.fields.names())

        rows = list(obj.rows())
        self.assertEqual(2, len(obj))
        self.assertEqual(2, len(rows))

        self.assertSequenceEqual([3.0, "Cecil"], rows[0])

    def test_store(self):
        store = XLSXStore(data_path("data.xlsx"))
        self.assertSequenceEqual(["amounts", "numbers"], store.object_names())

        obj = store.get_object("numbers", skip_rows=2)
        self.assertEqual(10, len(obj))

        store = open_store("xlsx", data_path("data.xlsx"))

if __name__ == "__main__":
    unittest.main()
