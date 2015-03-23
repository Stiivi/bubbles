import unittest
from ..common import data_path

from bubbles import FieldList, OperationContext
from bubbles.errors import ProbeAssertionError
from bubbles.backends.sql.objects import SQLDataStore
import bubbles.backends.sql.ops

class SQLBackendTestCase(unittest.TestCase):
    def setUp(self):
        self.context = OperationContext()
        self.context.add_operations_from(bubbles.backends.sql.ops)

        self.sql_data_store = SQLDataStore('sqlite:///')
        self.table = self.sql_data_store.create(
            'test',
            FieldList(('a', 'integer'), ('b', 'integer'), ('c', 'integer')),
            replace=True)
        self.table.append_from_iterable([(1,2,3), (1,2,4), (1,3,5)])

    def test_distinct(self):
        result = self.context.op.distinct(self.table)
        self.assertEqual(3, len(list(result.rows())))

    def test_assert_unique(self):
        self.context.op.assert_unique(self.table, 'c')

        with self.assertRaises(ProbeAssertionError):
            self.context.op.assert_unique(self.table, 'a')

    def test_assert_contains(self):
        self.context.op.assert_contains(self.table, 'a', 1)

        with self.assertRaises(ProbeAssertionError):
            self.context.op.assert_contains(self.table, 'a', 2)

    def test_assert_missing(self):
        self.context.op.assert_missing(self.table, 'a', 3)

        with self.assertRaises(ProbeAssertionError):
            self.context.op.assert_missing(self.table, 'a', 1)
