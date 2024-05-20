import imp
from django.conf import settings
from django.db import DatabaseError
from django.db.backends.base.operations import BaseDatabaseOperations
from django.db.backends.utils import split_tzname_delta
from django.utils import timezone
from itertools import chain
from datetime import date, datetime,timedelta
from django.utils.encoding import force_str
from django.utils.dateparse import parse_date, parse_datetime, parse_time
try:
    from django.db.backends.base.base import timezone_constructor # Django 4.2
except ImportError:
    from django.utils.timezone import timezone as timezone_constructor # Django 5+

from .utils import BulkInsertMapper

class DatabaseOperations(BaseDatabaseOperations):

    compiler_module = "django_iris.compiler"

    cast_data_types = {
        "TextField": "VARCHAR"
    }

    def quote_name(self, name):
        if name.startswith('"') and name.endswith('"'):
            return name  # Quoting once is enough.
        if '.' in name:
            return ".".join(['"%s"' % n for n in name.split('.')])
        return '"%s"' % name

    # def last_insert_id(self, cursor, table_name, pk_name):
    #     cursor.execute("SELECT TOP 1 %(pk)s FROM %(table)s ORDER BY %(pk)s DESC" % {
    #         'table': table_name,
    #         'pk': pk_name,
    #     })
    #     return cursor.fetchone()[0]

    def sql_flush(self, style, tables, *, reset_sequences=False, allow_cascade=False):
        if not tables:
            return []

        sql = []
        if reset_sequences:
            # It's faster to TRUNCATE tables that require a sequence reset
            # since ALTER TABLE AUTO_INCREMENT is slower than TRUNCATE.
            sql.extend(
                "%s %s %s;"
                % (
                    style.SQL_KEYWORD("TRUNCATE"),
                    style.SQL_KEYWORD("TABLE"),
                    style.SQL_FIELD(self.quote_name(table_name)),
                )
                for table_name in tables
            )
        else:
            # Otherwise issue a simple DELETE since it's faster than TRUNCATE
            # and preserves sequences.
            sql.extend(
                "%s %s %s;"
                % (
                    style.SQL_KEYWORD("DELETE"),
                    style.SQL_KEYWORD("FROM"),
                    style.SQL_FIELD(self.quote_name(table_name)),
                )
                for table_name in tables
            )
        
        return sql

    def no_limit_value(self):
        return None

    def limit_offset_sql(self, low_mark, high_mark):
        return ''

    def adapt_datetimefield_value(self, value):
        if value is None:
            return None       # Expression values are adapted by the database.

        if hasattr(value, 'resolve_expression'):
            return value

        if timezone.is_aware(value):
            if settings.USE_TZ:
                value = timezone.make_naive(value, self.connection.timezone)
            else:
                raise ValueError(
                    "IRIS backend does not support timezone-aware datetimes when "
                    "USE_TZ is False."
                )

        return str(value)
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
        elif internal_type == "TimeField":
            converters.append(self.convert_timefield_value)
        elif internal_type == "DateField":
            converters.append(self.convert_datefield_value)
        elif internal_type == "BooleanField":
            converters.append(self.convert_booleanfield_value)
        return converters
    
    def convert_booleanfield_value(self, value, expression, connection):
        if value in (0, 1):
            value = bool(value)
        return value


    def convert_datetimefield_value(self, value, expression, connection):
        original = value
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
        if value is not None:
            if settings.USE_TZ and not timezone.is_aware(value):
                value = timezone.make_aware(value, self.connection.timezone)
        return value

    def convert_datefield_value(self, value, expression, connection):
        if value is not None:
            if not isinstance(value, type(datetime.date)):
                try:
                    value = int(value)
                    value=date.fromordinal(672046+value)
                except:
                    value = parse_date(value)
        return value

    def convert_timefield_value(self, value, expression, connection):
        if value is not None:
            if not isinstance(value, type(datetime.time)):
                try:
                    value = int(value)
                    value = timedelta(seconds=value)
                    value = (datetime.min + value).time()
                except:
                    value = parse_time(value)
        return value

    def conditional_expression_supported_in_where_clause(self, expression):
        return False

    def adapt_datefield_value(self, value):
        if value == None:
            return None
        return str(value)

    def adapt_timefield_value(self, value):
        if value is None:
            return None

        if timezone.is_aware(value):
            raise ValueError("IRIS backend does not support timezone-aware times.")

        return str(value)

    # def field_cast_sql(self, db_type, internal_type):
    #     """
    #     Given a column type (e.g. 'BLOB', 'VARCHAR') and an internal type
    #     (e.g. 'GenericIPAddressField'), return the SQL to cast it before using
    #     it in a WHERE statement. The resulting string should contain a '%s'
    #     placeholder for the column being searched against.

    #     IRIS cannot work with streams directly in WHERE, wrap it with CONVERT
    #     """
    #     if db_type in ('TEXT', 'LONG BINARY'):
    #         return "CONVERT(VARCHAR, %s)"
    #     return "%s"

    def lookup_cast(self, lookup_type, internal_type=None):
        if lookup_type in ('TEXT', 'LONG BINARY'):
            return "CONVERT(VARCHAR, %s)"
        return "%s"

    def max_name_length(self):
        """
        Return the maximum length of table and column names, or None if there
        is no limit.
        """
        return 60
    
    def date_extract_sql(self, lookup_type, sql, params):
        if lookup_type == "week_day":
            param = 'dw'
        elif lookup_type == "iso_week_day":
            param = 'dw'
            return f"DATEPART({param}, {sql}) - 1", params
        elif lookup_type == "iso_year":
            param = 'yyyy'
        else:
            param = lookup_type
        return f"DATEPART({param}, CAST({sql} AS TIMESTAMP))", params

    def datetime_extract_sql(self, lookup_type, sql, params, tzname):
        sql, params = self._convert_sql_to_tz(sql, params, tzname)
        return self.date_extract_sql(lookup_type, sql, params)

    def time_extract_sql(self, lookup_type, sql, params):
        """
        Given a lookup_type of 'hour', 'minute', or 'second', return the SQL
        that extracts a value from the given time field field_name.
        """
        return self.date_extract_sql(lookup_type, sql, params)

    def _prepare_tzname_delta(self, tzname):
        tzname, sign, offset = split_tzname_delta(tzname)
        return f"{sign}{offset}" if offset else tzname

    def _convert_sql_to_tz(self, sql, params, tzname):
        if tzname and settings.USE_TZ and self.connection.timezone_name != tzname:
            try:
                value = datetime.now()
                value1 = timezone.make_aware(value, self.connection.timezone)
                value2 = timezone.make_aware(value, timezone_constructor(tzname))
                diff = -(value2 - value1).seconds
                return f"DATEADD(%s, %s, {sql})", (
                    "second",
                    diff,
                    *params,
                )
            except:
                pass
        return sql, params

    def datetime_cast_date_sql(self, sql, params, tzname):
        sql, params = self._convert_sql_to_tz(sql, params, tzname)
        return f"DATE({sql})", params

    def datetime_cast_time_sql(self, sql, params, tzname):
        sql, params = self._convert_sql_to_tz(sql, params, tzname)
        return f"TIME({sql})", params

    def date_trunc_sql(self, lookup_type, sql, params, tzname=None):
        sql, params = self._convert_sql_to_tz(sql, params, tzname)
        sql = f"CAST({sql} as DATE)"
        
        if lookup_type == 'year':
            return f"CAST(TO_CHAR(DATE({sql}), 'YYYY-01-01') AS DATE)", params
        if lookup_type == 'month':
            return f"CAST(TO_CHAR(DATE({sql}), 'YYYY-MM-01') AS DATE)", params
        if lookup_type == "week":
            return (
                f"CAST(DATEADD(DAY, - ((DATEPART(WEEKDAY, {sql}) + 5) # 7 ), {sql}) AS DATE)"
            ), (*params, )
       

        return f"DATE({sql})", params

    def datetime_trunc_sql(self, lookup_type, sql, params, tzname=None):
        sql, params = self._convert_sql_to_tz(sql, params, tzname)
        sql = f"CAST({sql} as TIMESTAMP)"

        fields = ["year", "month", "day", "hour", "minute", "second"]
        format = ("YYYY-", "MM", "-DD", " HH24:", "MI", ":SS")
        format_def = ("0000-", "01", "-01", " 00:", "00", ":00")

        if lookup_type == "week":
            format_str = "YYYY-MM-DD 00:00:00"
            return (
                f"CAST(TO_CHAR(DATEADD(DAY, - ((DATEPART(WEEKDAY, {sql}) + 5) # 7 ), {sql}), %s) AS DATETIME)"
            ), (*params, format_str)

        try:
            i = fields.index(lookup_type) + 1
        except ValueError:
            pass
        else:
            format_str = "".join(format[:i] + format_def[i:])
            return f"CAST(TO_CHAR({sql}, %s) AS TIMESTAMP)", (*params, format_str)
        
        return sql, params

    def time_trunc_sql(self, lookup_type, sql, params, tzname=None):
        sql = f"CAST({sql} as TIME)"

        fields = {
            "hour": "HH24:00:00",
            "minute": "HH24:MI:00",
            "second": "HH24:MI:SS",
        }
        if lookup_type in fields:
            format_str = fields[lookup_type]
            return f"CAST(TO_CHAR({sql}, %s) AS TIME)", (*params, format_str)
        else:
            return f"TIME({sql})", params

    def last_executed_query(self, cursor, sql, params):
        statement = cursor._parsed_statement
        if statement:
            last_params = cursor._params.collect()
            if last_params and len(last_params) > 0:
                for i in range(len(last_params)):
                    statement = statement.replace(f":%qpar({i+1})", 'NULL' if last_params[i] is None else repr(last_params[i]))
            statement = statement.replace(" . ", ".")
            statement = statement.replace(" ,", ",")
            statement = statement.replace(" )", ")")
            statement = statement.replace("( ", "(")
        return statement

    def insert_statement(self, on_conflict=None):
        if self.connection._disable_constraint_checking:
            return "INSERT %%NOCHECK INTO"
        return "INSERT INTO"

    def format_for_duration_arithmetic(self, sql):
        """Do nothing since formatting is handled in the custom function."""
        return sql

    def bulk_insert_sql(self, fields, placeholder_rows):
        query = []
        for row in placeholder_rows:
            select = []
            for i, placeholder in enumerate(row):
                # A model without any fields has fields=[None].
                if fields[i]:
                    internal_type = getattr(
                        fields[i], "target_field", fields[i]
                    ).get_internal_type()
                    placeholder = (
                        BulkInsertMapper.types.get(internal_type, "%s") % placeholder
                    )
                select.append(placeholder)
            query.append("SELECT %s " % ", ".join(select))
        # Bulk insert to tables with Oracle identity columns causes Oracle to
        # add sequence.nextval to it. Sequence.nextval cannot be used with the
        # UNION operator. To prevent incorrect SQL, move UNION to a subquery.
        return "SELECT * FROM (%s)" % " UNION ALL ".join(query)

    def combine_duration_expression(self, connector, sub_expressions):
        if connector not in ["+", "-"]:
            raise DatabaseError("Invalid connector for timedelta: %s." % connector)
        fn_params = ["microsecond"] + sub_expressions[::-1]
        if len(sub_expressions) > 3:
            raise ValueError("Too many params for timedelta operations.")
        return "DATEADD(%s)" % ", ".join(fn_params)
