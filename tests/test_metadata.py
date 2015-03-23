import unittest
from copy import copy
from bubbles import FieldList, Field, FieldFilter, to_field, prepare_aggregation_list
from bubbles.errors import *

class FieldListTestCase(unittest.TestCase):
    def test_list_creation(self):
        fields = FieldList("foo", "bar")

        for field in fields:
            self.assertEqual(type(field), Field)
            self.assertIsInstance(field.name, str)

        self.assertEqual("foo", fields[0].name, 'message')
        self.assertEqual(2, len(fields))

    def test_list_add(self):
        fields = FieldList("foo", "bar")
        fields.append("baz")
        self.assertEqual(3, len(fields))

    def test_indexes(self):
        fields = FieldList("a", "b", "c", "d")
        indexes = fields.indexes(["a", "c", "d"])
        self.assertEqual((0,2,3), indexes)

        indexes = fields.indexes( fields.fields() )
        self.assertEqual((0,1,2,3), indexes)

    def test_deletion(self):
        fields = FieldList("a", "b", "c", "d")
        del fields[0]

        self.assertEqual(["b", "c", "d"], fields.names())

        del fields[2]
        self.assertEqual(["b", "c"], fields.names())

        self.assertRaises(NoSuchFieldError, fields.field, "d")
        self.assertEqual(2, len(fields))

    def test_contains(self):
        fields = FieldList("a", "b", "c", "d")
        field = Field("a")
        self.assertIn("a", fields)
        self.assertIn(field, fields._fields)

    def test_aggregated_fields(self):
        fields = FieldList("a", "b")
        agg_list = prepare_aggregation_list(['a', ('b', 'avg')])
        agg_fields = fields.aggregated_fields(agg_list)
        self.assertListEqual(
            ['a_sum', 'b_avg', 'record_count'], agg_fields.names())

    #def test_retype(self):
    #    fields = FieldList(["a", "b", "c", "d"])
    #    self.assertEqual("unknown", fields.field("a").storage_type)
    #    retype_dict = {"a": {"storage_type":"integer"}}
    #    fields.retype(retype_dict)
    #    self.assertEqual("integer", fields.field("a").storage_type)

    #    retype_dict = {"a": {"name":"foo"}}
    #    self.assertRaises(Exception, fields.retype, retype_dict)

    def test_mask(self):
        fields = FieldList("a", "b", "c", "d")
        mask = fields.mask(["b", "d"])
        self.assertEqual([False, True, False, True], mask)

class MetadataTestCase(unittest.TestCase):
    def test_names(self):
        field = Field("bar")
        self.assertEqual("bar", field.name)
        self.assertEqual("bar", str(field))

    def test_to_field(self):
        field = to_field("foo")
        self.assertIsInstance(field, Field)
        self.assertEqual("foo", field.name)
        self.assertIsInstance(field.name, str)
        # self.assertEqual("unknown", field.storage_type)
        # self.assertEqual("typeless", field.analytical_type)

        field = to_field(["bar", "string", "flag"])
        self.assertEqual("bar", field.name)
        self.assertEqual("string", field.storage_type)
        self.assertEqual("flag", field.analytical_type)

        desc = {
                "name":"baz",
                "storage_type":"integer",
                "analytical_type": "flag"
            }
        field = to_field(desc)
        self.assertEqual("baz", field.name)
        self.assertEqual("integer", field.storage_type)
        self.assertEqual("flag", field.analytical_type)

    def test_field_to_dict(self):
        desc = {
                "name":"baz",
                "storage_type":"integer",
                "analytical_type": "flag"
            }
        field = to_field(desc)
        field2 = to_field(field.to_dict())
        self.assertEqual(field, field2)

    @unittest.skip("skipping")
    def test_coalesce_value(self):
        self.assertEqual(1, coalesce_value("1", "integer"))
        self.assertEqual("1", coalesce_value(1, "string"))
        self.assertEqual(1.5, coalesce_value("1.5", "float"))
        self.assertEqual(1000, coalesce_value("1 000", "integer", strip=True))
        self.assertEqual(['1','2','3'], coalesce_value("1,2,3", "list", strip=True))


    def setUp(self):
        self.fields = FieldList("a", "b", "c", "d")

    def test_init(self):
        self.assertRaises(MetadataError, FieldFilter, drop=["foo"], keep=["bar"])

    def test_map(self):

        m = FieldFilter(drop=["a","c"])
        self.assertListEqual(["b", "d"], m.filter(self.fields).names())

        m = FieldFilter(keep=["a","c"])
        self.assertListEqual(["a", "c"], m.filter(self.fields).names())

        m = FieldFilter(rename={"a":"x","c":"y"})
        self.assertListEqual(["x", "b", "y", "d"], m.filter(self.fields).names())

    def test_selectors(self):
        m = FieldFilter(keep=["a","c"])
        self.assertListEqual([True, False, True, False],
                                m.field_mask(self.fields))

        m = FieldFilter(drop=["b","d"])
        self.assertListEqual([True, False, True, False],
                                m.field_mask(self.fields))

        m = FieldFilter()
        self.assertListEqual([True, True, True, True],
                                m.field_mask(self.fields))
def test_suite():
   suite = unittest.TestSuite()

   suite.addTest(unittest.makeSuite(FieldListTestCase))
   suite.addTest(unittest.makeSuite(MetadataTestCase))

   return suite
