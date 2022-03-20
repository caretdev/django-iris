from decimal import Decimal

from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.db import NotSupportedError


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):
    sql_alter_column_type = "ALTER COLUMN %(column)s %(type)s"
    sql_alter_column_null = "ALTER COLUMN %(column)s NULL"

    # Fix for correct alter column syntax
    # ALTER TABLE tablename ALTER COLUMN oldname RENAME newname
    sql_rename_column = (
        "ALTER TABLE %(table)s ALTER COLUMN %(old_column)s RENAME %(new_column)s"
    )

    sql_rename_table = "ALTER TABLE %(old_table)s RENAME %(new_table)s"

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

    def skip_default_on_alter(self, field):
        """
        Some backends don't accept default values for certain columns types
        (i.e. MySQL longtext and longblob) in the ALTER COLUMN statement.
        """
        return True

    # Due to DP-412796 and DP-412798 disable default fields
    # on alter statements
    def skip_default(self, field):
        # return super().skip_default(field)
        return True

    # def _rename_field_sql(self, table, old_field, new_field, new_type):
    #     return self.sql_rename_column % {
    #         "table": self.quote_name(table),
    #         "old_column": self.quote_name(old_field.column),
    #         "new_column": self.quote_name(new_field.column),
    #         "type": new_type,
    #     }