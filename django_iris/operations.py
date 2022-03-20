import imp
from django.conf import settings
from django.db.backends.base.operations import BaseDatabaseOperations
from django.utils import timezone
from itertools import chain
from datetime import datetime

import traceback

class DatabaseOperations(BaseDatabaseOperations):

    # compiler_module = "django_iris.compiler"

    def quote_name(self, name):
        if name.startswith('"') and name.endswith('"'):
            return name  # Quoting once is enough.
        return '"%s"' % name

    def last_insert_id(self, cursor, table_name, pk_name):
        cursor.execute("SELECT TOP 1 %(pk)s FROM %(table)s ORDER BY %(pk)s DESC" % {
            'table': table_name,
            'pk': pk_name,
        })
        return cursor.fetchone()[0]

    def sql_flush(self, style, tables, *, reset_sequences=False, allow_cascade=False):
        if tables and allow_cascade:
            # Simulate TRUNCATE CASCADE by recursively collecting the tables
            # referencing the tables to be flushed.
            tables = set(chain.from_iterable(
                self._references_graph(table) for table in tables))
        sql = ['%s %s %s;' % (
            style.SQL_KEYWORD('DELETE'),
            style.SQL_KEYWORD('FROM'),
            style.SQL_FIELD(self.quote_name(table))
        ) for table in tables]
        if reset_sequences:
            sequences = [{'table': table} for table in tables]
            sql.extend(self.sequence_reset_by_name_sql(style, sequences))
        return sql

    def no_limit_value(self):
        return None

    def limit_offset_sql(self, low_mark, high_mark):
        # print("limit_offset_sql")
        limit, offset = self._get_limit_offset_params(low_mark, high_mark)
        return ' '.join(sql for sql in (
            # ('LIMIT %d' % limit) if limit else None,
            # ('OFFSET %d' % offset) if offset else None,
        ) if sql)

    def adapt_datetimefield_value(self, value):
        # print('adapt_datetimefield_value', value)
        # print(*traceback.format_stack(), sep='\n')
        if value is None:
            return None
        # Expression values are adapted by the database.
        if hasattr(value, 'resolve_expression'):
            return value

        value = int(value.timestamp() * 1000000)
        if value >= 0:
            value += 2 ** 60
        else:
            value += -(2 ** 61 * 3)
        
        return str(value)
        # return str(value).split("+")[0]

    def get_db_converters(self, expression):
        converters = super().get_db_converters(expression)
        internal_type = expression.output_field.get_internal_type()
        if internal_type == "DateTimeField":
            converters.append(self.convert_datetimefield_value)
        return converters

    def convert_datetimefield_value(self, value, expression, connection):
        # print('convert_datetimefield_value', value, expression)
        if isinstance(value, int):
            if value > 0:
                value -= 2 ** 60
            else:
                value -= -(2 ** 61 * 3)
            value = value / 1000000
            value = datetime.fromtimestamp(value)
        return value
