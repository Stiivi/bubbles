import unittest
from ..common import data_path

from bubbles.errors import *
from bubbles.backends.text.objects import CSVSource, CSVTarget
from bubbles.metadata import FieldList

class TextBackendTestCase(unittest.TestCase):
    def test_load(self):
        obj = CSVSource(data_path("fruits-sk.csv"))
        self.assertEqual(["id", "fruit", "type"], obj.fields.names())

        rows = list(obj.rows())
        self.assertEqual(16, len(rows))

        self.assertEqual(["1", "jablko", "malvice"], rows[0])
        obj.release()

    def test_infer_types(self):
        obj = CSVSource(data_path("fruits-sk.csv"), infer_fields=True)
        self.assertEqual("integer", obj.fields[0].storage_type)

        rows = list(obj.rows())
        self.assertEqual([1, "jablko", "malvice"], rows[0])
        obj.release()

    def test_no_header(self):
        with self.assertRaises(ArgumentError):
            obj = CSVSource(data_path("fruits-sk.csv"), read_header=False)

        fields = FieldList("id", "fruit", "type")
        obj = CSVSource(data_path("fruits-sk.csv"), read_header=False,
                                fields=fields)
        self.assertEqual(["id", "fruit", "type"], obj.fields.names())

        rows = list(obj.rows())
        self.assertEqual(17, len(rows))

        self.assertEqual(["id", "fruit", "type"], rows[0])
        obj.release()

    def test_encoding(self):
        obj_l2 = CSVSource(data_path("fruits-sk-latin2.csv"), encoding="latin2")
        rows_l2 = list(obj_l2.rows())
        obj_l2.release()

        obj_utf = CSVSource(data_path("fruits-sk.csv"))
        rows_utf = list(obj_utf.rows())
        obj_utf.release()
        self.assertEqual(rows_l2, rows_utf)

if __name__ == "__main__":
    unittest.main()
