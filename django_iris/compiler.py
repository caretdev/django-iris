from django.core.exceptions import FieldError
from django.db.models.expressions import Col
from django.db.models.sql import compiler

class SQLCompiler(compiler.SQLCompiler):
    def as_subquery_condition(self, alias: str, columns, compiler):
        return super().as_subquery_condition(alias, columns, compiler)


class SQLInsertCompiler(compiler.SQLInsertCompiler, SQLCompiler):
    def as_sql(self):
        return super().as_sql()


class SQLDeleteCompiler(compiler.SQLDeleteCompiler, SQLCompiler):
    def as_sql(self):
        return super().as_sql()


class SQLUpdateCompiler(compiler.SQLUpdateCompiler, SQLCompiler):
    def as_sql(self):
        return super().as_sql()


class SQLAggregateCompiler(compiler.SQLAggregateCompiler, SQLCompiler):
    def as_sql(self):
        return super().as_sql()