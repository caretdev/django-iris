from decimal import Decimal

from django.db.backends.base.schema import BaseDatabaseSchemaEditor


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):
    sql_alter_column_type = "ALTER COLUMN %(column)s %(type)s"
    sql_alter_column_null = "ALTER COLUMN %(column)s NULL"

    def quote_value(self, value):
        if isinstance(value, bool):
            return str(int(value))
        elif isinstance(value, (Decimal, float, int)):
            return str(value)
        elif isinstance(value, str):
            return "'%s'" % value.replace("\'", "\'\'")
        elif value is None:
            return "NULL"
        return value

    def prepare_default(self, value):
        raise NotImplementedError()
