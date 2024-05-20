from decimal import Decimal

from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.db import NotSupportedError, models


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):

    sql_create_table = (
        "CREATE TABLE %(table)s (%(definition)s)"
        "WITH %%%%CLASSPARAMETER ALLOWIDENTITYINSERT = 1"
    )

    sql_alter_column_type = "ALTER COLUMN %(column)s %(type)s"
    sql_alter_column_collation = "ALTER COLUMN %(column)s %(collation)s"
    sql_alter_column_null = "ALTER COLUMN %(column)s NULL"
    sql_alter_column_not_null = "ALTER COLUMN %(column)s NOT NULL"
    sql_alter_column_default = "ALTER COLUMN %(column)s DEFAULT %(default)s"

    sql_delete_index = "DROP INDEX IF EXISTS %(name)s"

    # Fix for correct alter column syntax
    # ALTER TABLE tablename ALTER COLUMN oldname RENAME newname
    sql_rename_column = (
        "ALTER TABLE %(table)s ALTER COLUMN %(old_column)s RENAME %(new_column)s"
    )

    sql_rename_table = "ALTER TABLE %(old_table)s RENAME %(new_table)s"

    sql_create_fk = (
        "ALTER TABLE %(table)s ADD CONSTRAINT %(name)s FOREIGN KEY (%(column)s) "
        "REFERENCES %(to_table)s (%(to_column)s)%(deferrable)s"
    )
    sql_create_unique = "CREATE UNIQUE INDEX %(name)s ON %(table)s (%(columns)s)"

    def quote_value(self, value):
        if isinstance(value, bool):
            return str(int(value))
        elif isinstance(value, (Decimal, float, int)):
            return str(value)
        elif isinstance(value, str):
            return "'%s'" % value.replace("'", "''")
        elif value is None:
            return "NULL"
        elif isinstance(value, bytes):
            return "'%s'" % value.decode().replace("'", "''")
        return value

    def quote_name(self, name):
        # if '.' in name:
        #     return name
        return self.connection.ops.quote_name(name)

    def prepare_default(self, value):
        return self.quote_value(value)

    # def table_sql(self, model):
    #     if '.' in model._meta.db_table:
    #         raise NotSupportedError(
    #             "Invalid table name '%s'" % (model._meta.db_table)
    #         )
    #     return super().table_sql(model)

    def _create_fk_sql(self, model, field, suffix):
        on_delete = getattr(field.remote_field, "on_delete", None)
        stmt = super()._create_fk_sql(model, field, suffix)
        if on_delete == models.CASCADE:
            stmt = str(stmt) + " ON DELETE CASCADE"
        if on_delete == models.SET_NULL:
            stmt = str(stmt) + " ON DELETE SET NULL"
        return stmt

    # def _rename_field_sql(self, table, old_field, new_field, new_type):
    #     return self.sql_rename_column % {
    #         "table": self.quote_name(table),
    #         "old_column": self.quote_name(old_field.column),
    #         "new_column": self.quote_name(new_field.column),
    #         "type": new_type,
    #     }

    def _collate_sql(self, collation, old_collation=None, table_name=None):
        return f"COLLATE {collation}"

    def _alter_column_type_sql(
        self, model, old_field, new_field, new_type, old_collation, new_collation
    ):

        action, other_actions = super()._alter_column_type_sql(
            model, old_field, new_field, new_type, None, None
        )
        if not new_collation:
            return action, other_actions

        if collate_sql := self._collate_sql(
            new_collation, old_collation, model._meta.db_table
        ):
            collate_sql = f" {collate_sql}"
        else:
            collate_sql = ""

        other_actions += [
            (
                self.sql_alter_column
                % {
                    "table": self.quote_name(model._meta.db_table),
                    "changes": (
                        self.sql_alter_column_collation
                        % {
                            "column": self.quote_name(new_field.column),
                            "collation": collate_sql,
                        }
                    ),
                },
                [],
            )
        ]

        return action, other_actions

    def _index_condition_sql(self, condition):
        return ""
