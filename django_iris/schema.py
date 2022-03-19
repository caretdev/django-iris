from decimal import Decimal

from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.db import NotSupportedError

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
        return self.quote_value(value)

    def table_sql(self, model):
        if '.' in model._meta.db_table:
            raise NotSupportedError(
                "Invalid table name '%s'" % (model._meta.db_table)
            )
        return super().table_sql(model)