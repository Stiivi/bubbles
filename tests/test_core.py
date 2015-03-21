import unittest
from bubbles import *
# import bubbles.iterator

# FIXME: clean this up
import inspect

def default(ctx, left):
    pass

def unary(ctx, left):
    pass

def binary(ctx, left, right):
    pass

class DummyDataObject(DataObject):
    def __init__(self, reps=None, data=None):
        """Creates a dummy data object with bogus representations `reps` and
        arbitrary data `data`"""
        self.reps = reps or []
        self.data = data

    def representations(self):
        return self.reps

class TextObject(DataObject):
    def __init__(self, string):
        self.string = string

    def representations(self):
        return ["rows", "text"]

    def rows(self):
        return iter(self.string)

    def text(self):
        return self.string

class OperationTestCase(unittest.TestCase):
    def test_match(self):
        self.assertTrue(Signature("sql").matches("sql"))
        self.assertTrue(Signature("*").matches("sql"))
        self.assertTrue(Signature("sql[]").matches("sql[]"))
        self.assertTrue(Signature("*[]").matches("sql[]"))

        self.assertFalse(Signature("sql").matches("rows"))
        self.assertFalse(Signature("sql").matches("sql[]"))

    def test_common_reps(self):
        objs = [
                DummyDataObject(["a", "b", "c"]),
                DummyDataObject(["a", "b", "d"]),
                DummyDataObject(["b", "d", "e"])
            ]
        self.assertEqual(["b"], list(common_representations(*objs)))

        objs = [
                DummyDataObject(["a", "b", "c"]),
                DummyDataObject(["a", "b", "d"]),
                DummyDataObject(["d", "d", "e"])
            ]
        self.assertEqual([], list(common_representations(*objs)))

    def test_prototype(self):
        proto = Signature("sql", "sql")
        match = Signature("*", "*")
        self.assertEqual(match, proto.as_prototype())

        proto = Signature("sql[]", "sql")
        match = Signature("*[]", "*")
        self.assertEqual(match, proto.as_prototype())

        proto = Signature("*[]", "*")
        match = Signature("*[]", "*")
        self.assertEqual(match, proto.as_prototype())

    def test_create_op(self):
        def one(ctx, obj):
            pass
        def fun(ctx, left, right):
            pass

        with self.assertRaises(ArgumentError):
            Operation("fun", [])

        op = Operation("fun")
        self.assertEqual(1, op.opcount)
        self.assertEqual(["obj"], op.operands)

        op = Operation("fun", ["left", "right"])
        self.assertEqual(2, op.opcount)

    def test_register(self):
        def func(ctx, obj):
            pass

        def func_sql(ctx, obj):
            pass

        def func_text(ctx, obj):
            pass

        op = Operation("select", ["obj"])

        op.register(func)
        self.assertEqual(1, len(op.registry))
        self.assertEqual([Signature("*")], list(op.registry.keys()))

        op.register(func_sql, "sql")
        op.register(func_text, "text")
        self.assertEqual(3, len(op.registry))

        sigs = []
        for s in op.signatures():
            sigs.append([op.rep for op in s.operands])

        self.assertSequenceEqual(sigs, [["*"], ["sql"], ["text"]])

        f = op.function(Signature("*"))
        self.assertEqual(f, func)
        f = op.function(Signature("sql"))
        self.assertEqual(f, func_sql)
        f = op.function(Signature("text"))
        self.assertEqual(f, func_text)

    def test_register_invalid(self):
        def func_invalid(obj):
            pass

        def func_invalid2(ctx, obj):
            pass

        op = Operation("select")

        with self.assertRaisesRegex(ArgumentError, "Expected at least"):
            op.register(func_invalid)

    def test_decorator(self):

        op = Operation("select", ["obj"])

        @op.register
        def func(ctx, obj):
            pass

        sig = Signature("*")
        self.assertEqual(1, len(op.registry))
        self.assertEqual([sig], list(op.registry.keys()))
        self.assertEqual(func, op.registry[sig])

        @op.register("sql")
        def func_sql(ctx, obj):
            pass

        sig = Signature("sql")
        self.assertEqual(2, len(op.registry))
        self.assertEqual(func_sql, op.registry[sig])

        # Test decorator implicitly creating Operation
        @operation
        def implicit(ctx, obj):
            pass

        self.assertIsInstance(implicit, Operation)
        self.assertEqual("implicit", implicit.name)

    def test_context(self):
        c = OperationContext()

        with self.assertRaises(KeyError):
            c.operations["select"]

        with self.assertRaises(OperationError):
            c.operation("select")

        c.add_operation(Operation("select"))
        op = c.operations["select"]

        @operation
        def touch(ctx, obj):
            pass

        c.add_operation(touch)
        op = c.operations["touch"]

    def test_resolution_order(self):
        @operation
        def upper(ctx, obj):
            pass

        obj = DummyDataObject(["rows"])

        order = upper.resolution_order(get_representations(obj))
        self.assertEqual([Signature("*")], order)

        @upper.register("rows")
        def _(ctx, obj):
            pass

        order = upper.resolution_order(get_representations(obj))
        self.assertEqual([Signature("rows"),
                          Signature("*")], order)

    def test_call(self):
        @operation
        def upper(ctx, obj):
            return obj.text().upper()

        c = OperationContext()
        c.add_operation(upper)

        obj = TextObject("hi there")
        result = c.call("upper", obj)
        self.assertEqual("HI THERE", result)

    def test_call_context_op(self):
        op = Operation("upper")
        @op.register("rows")
        def _(ctx, obj):
            rows = obj.rows()
            text = "".join(rows)
            return list(text.upper())

        @op.register("text")
        def _(ctx, obj):
            text = obj.text()
            return list(text.upper())


        c = OperationContext()
        c.add_operation(op)

        obj = TextObject("windchimes")

        result = c.op.upper(obj)
        self.assertEqual(list("WINDCHIMES"), result)

    def test_get_representations(self):
        obj = DummyDataObject(["rows", "sql"])
        self.assertEqual( [["rows", "sql"]], get_representations(obj))

        obj = DummyDataObject(["rows", "sql"])
        extr = get_representations([obj])
        self.assertEqual( [["rows[]", "sql[]"]], extr)

    def test_comparison(self):
        sig1 = Signature("a", "b", "c")
        sig2 = Signature("a", "b", "c")
        sig3 = Signature("a", "b")

        self.assertTrue(sig1 == sig1)
        self.assertTrue(sig1 == sig2)
        self.assertFalse(sig1 == sig3)

        self.assertTrue(sig1 == ["a", "b", "c"])
        self.assertFalse(sig1 == ["a", "b"])

    def test_retry(self):
        op = Operation("join", ["left", "right"])

        @op.register("sql", "sql")
        def _(ctx, l, r):
            if l.data == r.data:
                return "SQL"
            else:
                raise RetryOperation(["sql", "rows"])

        @op.register("sql", "rows")
        def _(ctx, l, r):
            return "ITERATOR"

        local = DummyDataObject(["sql", "rows"], "local")
        remote = DummyDataObject(["sql", "rows"], "remote")

        c = OperationContext()
        c.add_operation(op)

        result = c.op.join(local, local)
        self.assertEqual(result, "SQL")

        result = c.op.join(local, remote)
        self.assertEqual(result, "ITERATOR")

        fail = Operation("fail", ["left", "right"])
        @fail.register("sql", "rows")
        def _(ctx, l, r):
            raise RetryOperation(["sql", "sql"])
        c.add_operation(fail)

        with self.assertRaises(OperationError):
            c.op.fail(local, local)

        # Already visited
        repeat = Operation("repeat", ["left", "right"])
        @repeat.register("sql", "sql")
        def _(ctx, l, r):
            raise RetryOperation(["sql", "sql"])
        c.add_operation(repeat)

        with self.assertRaises(RetryError):
            c.op.repeat(local, local)

    def test_allow_deny_retry(self):
        swim = Operation("swim", ["obj"])
        @swim.register("sql")
        def _(ctx, obj):
            raise RetryOperation(["rows"])

        @swim.register("rows")
        def _(ctx, obj):
            obj.data = "good"
            return obj

        obj = DummyDataObject(["sql", "rows"], "")

        c = OperationContext()
        c.add_operation(swim)

        result = c.op.swim(obj)
        self.assertEqual("good", result.data)

        c.retry_deny = ["swim"]
        c.retry_allow = []
        with self.assertRaises(RetryError):
            c.op.swim(obj)

        c.retry_deny = []
        c.retry_allow = ["swim"]
        result = c.op.swim(obj)
        self.assertEqual("good", result.data)

        c.retry_deny = ["swim"]
        c.retry_allow = ["swim"]
        with self.assertRaises(RetryError):
            c.op.swim(obj)


    def test_retry_nested(self):
        """Test whether failed nested operation fails correctly (Because of
        Issue #4)."""

        aggregate = Operation("aggregate", ["obj"])
        @aggregate.register("sql")
        def _(ctx, obj, fail):
            if fail:
                raise RetryOperation(["rows"])
            else:
                obj.data += "-SQL-"
            return obj

        @aggregate.register("rows")
        def _(ctx, obj, fail):
            obj.data += "-ROWS-"
            return obj

        window_aggregate = Operation("window_aggregate", ["obj"])
        @window_aggregate.register("sql")
        def _(ctx, obj, fail):
            obj.data += "START"
            ctx.op.aggregate(obj, fail)
            obj.data += "END"

        c = OperationContext()
        c.add_operation(aggregate)
        c.add_operation(window_aggregate)

        # Expected order:
        # 1. window_aggregate is called
        # 2. sql aggregate is called, but fails
        # 3. row aggregate is called
        # 4. window aggregate continues

        obj = DummyDataObject(["sql"], "")

        c.op.window_aggregate(obj, fail=True)
        self.assertEqual("START-ROWS-END", obj.data)

        obj.data = ""
        c.op.window_aggregate(obj, fail=False)
        self.assertEqual("START-SQL-END", obj.data)

    def test_priority(self):
        objsql = DummyDataObject(["sql", "rows"])
        objrows = DummyDataObject(["rows", "sql"])

        meditate = Operation("meditate", ["obj"])

        @meditate.register("sql")
        def fsql(ctx, obj):
            return "sql"

        @meditate.register("rows")
        def frows(ctx, obj):
            return "rows"

        c = OperationContext()
        c.add_operation(meditate)

        self.assertEqual("sql", c.op.meditate(objsql))
        self.assertEqual("rows", c.op.meditate(objrows))

if __name__ == "__main__":
    unittest.main()
