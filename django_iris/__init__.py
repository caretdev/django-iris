from django.db.models.functions.math import Random, Ln, Log
from django.db.models.functions.datetime import Now
from django.db.models.expressions import Exists, Func, Value, Col, OrderBy
from django.db.models.functions.text import Chr, ConcatPair, StrIndex
from django.db.models.functions import Cast
from django.db.models.fields import TextField, CharField
from django.db.models.lookups import BuiltinLookup
from django.db.models.fields.json import (
    KeyTransform,
    KeyTransformExact,
    compile_json_path,
)

from django_iris.compiler import SQLCompiler

fn_template = "{fn %(function)s(%(expressions)s)}"

as_fn = [
    "ACOS",
    "ASIN",
    "ATAN",
    "ATAN2",
    "COS",
    "COT",
    "EXP",
    "LN",
    "LOG",
    "LOG10",
    "PI",
    "SIN",
    "TAN",
]


def as_intersystems(cls):
    def inner(func):
        cls.as_intersystems = func

    return inner


class Log10(Func):
    function = "LOG10"
    arity = 1
    lookup_name = "log10"


class Convert(Func):
    function = "CONVERT"
    lookup_name = "convert"

    template = "%(function)s(%(db_type)s, %(expressions)s)"

    def __init__(self, expression, output_field):
        super().__init__(expression, output_field=output_field)

    def as_sql(self, compiler, connection, **extra_context):
        extra_context["db_type"] = self.output_field.cast_db_type(connection)
        return super().as_sql(compiler, connection, **extra_context)


def convert_streams(expressions):
    return [
        (
            Convert(expression, CharField())
            if isinstance(expression, Col) and isinstance(expression.target, TextField)
            else expression
        )
        for expression in expressions
    ]


@as_intersystems(Exists)
def exists_as_intersystems(self, compiler, connection, template=None, **extra_context):
    # template = "(SELECT COUNT(*) FROM (%(subquery)s))"
    template = "EXISTS %(subquery)s"
    return self.as_sql(compiler, connection, template, **extra_context)


@as_intersystems(Chr)
def chr_as_intersystems(self, compiler, connection, **extra_context):
    return self.as_sql(compiler, connection, function="CHAR", **extra_context)


@as_intersystems(ConcatPair)
def concat_as_intersystems(self, compiler, connection, **extra_context):
    copy = self.copy()
    expressions = convert_streams(copy.get_source_expressions())
    """
    STRING in IRIS retuns NULL if all NULL arguments, so, just add empty string, to make it always non NULL
    """
    copy.set_source_expressions([Value("")] + expressions)
    return super(ConcatPair, copy).as_sql(
        compiler,
        connection,
        function="STRING",
        **extra_context,
    )


@as_intersystems(StrIndex)
def instr_as_intersystems(self, compiler, connection, **extra_context):
    copy = self.copy()
    expressions = convert_streams(copy.get_source_expressions())
    copy.set_source_expressions(expressions)
    return super(StrIndex, copy).as_sql(
        compiler,
        connection,
        **extra_context,
    )


@as_intersystems(Random)
def random_as_intersystems(self, compiler, connection, **extra_context):
    return self.as_sql(
        compiler, connection, template="%%TSQL.ZRAND(1e10)", **extra_context
    )


@as_intersystems(Ln)
def ln_as_intersystems(self, compiler, connection, **extra_context):
    return self.as_sql(
        compiler, connection, function="LOG", template=fn_template, **extra_context
    )


@as_intersystems(Log)
def log_as_intersystems(self, compiler, connection, **extra_context):
    copy = self.copy()
    copy.set_source_expressions(
        [Log10(expression) for expression in copy.get_source_expressions()[::-1]]
    )
    return super(Log, copy).as_sql(
        compiler,
        connection,
        arg_joiner=" / ",
        template="%(expressions)s",
        **extra_context,
    )


@as_intersystems(Func)
def func_as_intersystems(self, compiler: SQLCompiler, connection, **extra_context):
    if self.function in as_fn:
        return self.as_sql(compiler, connection, template=fn_template, **extra_context)
    return self.as_sql(compiler, connection, **extra_context)


@as_intersystems(Now)
def now_as_intersystems(self, compiler, connection, **extra_context):
    return self.as_sql(
        compiler, connection, template="CURRENT_TIMESTAMP(6)", **extra_context
    )


@as_intersystems(OrderBy)
def orderby_as_intersystems(self, compiler, connection, **extra_context):
    copy = self.copy()
    # IRIS does not support order NULL
    copy.nulls_first = copy.nulls_last = False
    return copy.as_sql(compiler, connection, **extra_context)


@as_intersystems(KeyTransformExact)
def json_KeyTransformExact_as_intersystems(self, compiler, connection):
    return self.as_sql(compiler, connection)


@as_intersystems(KeyTransform)
def json_KeyTransform_as_intersystems(self, compiler, connection):
    # breakpoint()
    # json_path = compile_json_path(key_transforms)
    return f"{self.field.name}__{self.key_name}", []


@as_intersystems(BuiltinLookup)
def BuiltinLookup_as_intersystems(self, compiler, connection):
    sql, params = self.as_sql(compiler, connection)
    if compiler.in_get_order_by:
        return "CASE WHEN %s THEN 1 ELSE 0 END" % (sql,), params
    # if not compiler.in_get_select and not compiler.in_get_order_by:
    return sql, params


@as_intersystems(Cast)
def cast_as_intersystems(self, compiler, connection, **extra_context):
    if hasattr(self.source_expressions[0], "lookup_name"):
        if self.source_expressions[0].lookup_name in ["gt", "gte", "lt", "lte"]:
            return self.as_sql(
                compiler,
                connection,
                template="CASE WHEN %(expressions)s THEN 1 ELSE 0 END",
                **extra_context,
            )
    return self.as_sql(compiler, connection, **extra_context)
