from expressions import Compiler, ExpressionError, default_dialect

class bubbles_dialect(default_dialect):
    operators = {
        # "^": (1000, RIGHT, BINARY),
        "~": (1000, None, UNARY),
        "¬": (1000, None, UNARY),
        "*": (900, LEFT, BINARY),
        "/": (900, LEFT, BINARY),
        "%": (900, LEFT, BINARY),

        "+":  (500, LEFT, BINARY),
        "-":  (500, LEFT, UNARY | BINARY),

        "<<": (350, LEFT, BINARY),
        ">>": (350, LEFT, BINARY),

        "&":  (300, LEFT, BINARY),
        "|":  (300, LEFT, BINARY),

        "<":  (200, LEFT, BINARY),
        "≤": (200, LEFT, BINARY),
        "<=": (200, LEFT, BINARY),
        ">":  (200, LEFT, BINARY),
        "≥": (200, LEFT, BINARY),
        ">=": (200, LEFT, BINARY),
        "≠": (200, LEFT, BINARY),
        "!=": (200, LEFT, BINARY),
        "=":  (200, LEFT, BINARY),

        "not": (120, None, UNARY),
        "and": (110, LEFT, BINARY),
        "or":  (100, LEFT, BINARY),
    }
    case_sensitive = False

    # TODO: future function list (unused now)
    functions = (
            "min", "max",
            )

# IS blank
# IS null

# Translate bubbles operator to Python operator
_python_operators = {
    "=": "=="
}


class PythonExpressionCompiler(Compiler):
    def compile_literal(self, context, literal):
        # TODO: support string literals (quote them properely)
        if isinstance(literal, str):
            raise NotImplementedError("String literals are not yet supported")
        else:
            return str(literal)

    def compile_variable(self, context, variable):
        if variable in context:
            return variable
        else:
            raise ExpressionError("Unknown variable %s" % variable)

    def compile_operator(self, context, operator, op1, op2):
        operator = _python_operators.get(operator, operator)
        return "(%s%s%s)" % (op1, operator, op2)

    def compile_unary(self, context, operator, operand):
        operator = _python_operators.get(operator, operator)
        return "(%s%s)" % (operand, operator)

    def compile_function(self, context, function, args):
        # TODO: support functions
        raise NotImplementedError("Functions are not yet implemented")


def PythonLambdaCompiler(PythonExpressionCompiler):
    def finalize(self, context):
        # TODO: sanitize field names in context
        field_list = ", ".join(context)
        lambda_str = "lambda %s: %s" % (context, expression)
        return lambda_str


def lambda_from_predicate(predicate, key):
    compiler = PythonLambdaCompiler()
    expression = compiler.compile(predicate, context=key)
    return expression
