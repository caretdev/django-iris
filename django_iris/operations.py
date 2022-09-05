import imp
from django.conf import settings
from django.db.backends.base.operations import BaseDatabaseOperations
from django.utils import timezone
from itertools import chain
from datetime import date, datetime,timedelta

from django.utils.dateparse import parse_date, parse_datetime, parse_time


class DatabaseOperations(BaseDatabaseOperations):

    compiler_module = "django_iris.compiler"

    def quote_name(self, name):
        if name.startswith('"') and name.endswith('"'):
            return name  # Quoting once is enough.
        if '.' in name:
            return ".".join(['"%s"' % n for n in name.split('.')])
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
        return ''

    def adapt_datetimefield_value(self, value):
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
        if internal_type == "TimeField":
            converters.append(self.convert_timefield_value)
        if internal_type == "DateField":
            converters.append(self.convert_datefield_value)
        return converters

    def convert_datetimefield_value(self, value, expression, connection):
        if value is not None:
            if isinstance(value, int):
                if value > 0:
                    value -= 2 ** 60
                else:
                    value -= -(2 ** 61 * 3)
                value = value / 1000000
                value = datetime.fromtimestamp(value)
            elif isinstance(value, str):
                if value != '':
                    value = parse_datetime(value)
                    if settings.USE_TZ and not timezone.is_aware(value):
                        value = timezone.make_aware(
                            value, self.connection.timezone)
        return value

    def convert_datefield_value(self, value, expression, connection):
        if value is not None:
            if not isinstance(value, type(datetime.date)):
                if isinstance(value, int):
                    value=date.fromordinal(672046+value)
                else:
                    value = parse_date(value)
        return value

    def convert_timefield_value(self, value, expression, connection):
        if value is not None:
            if not isinstance(value, type(datetime.time)):
                if isinstance(value, int):
                    value=timedelta(seconds=value)
                else:
                    value = parse_time(value)
        return value

    def conditional_expression_supported_in_where_clause(self, expression):
        return False

    def adapt_datefield_value(self, value):
        print('adapt_datefield_value: ' + str(value))
        if value == None:
            return None
        return str(value)

    def adapt_datetimefield_value(self, value):
        print('adapt_datetimefield_value: ' + str(value))
        if value == None:
            return None
        return str(value)

    def adapt_timefield_value(self, value):
        if value is None:
            return None
        if timezone.is_aware(value):
            raise ValueError("Django does not support timezone-aware times.")
        return str(value)
