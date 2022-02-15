from django.db.backends.base.introspection import (
    BaseDatabaseIntrospection, FieldInfo as BaseFieldInfo, TableInfo,
)


class DatabaseIntrospection(BaseDatabaseIntrospection):

    def get_table_list(self, cursor):
        cursor.execute("""
          SELECT TABLE_NAME
          FROM information_schema.tables
          WHERE TABLE_SCHEMA = 'SQLUser'
        """)
        return [TableInfo(row[0], 't') for row in cursor.fetchall()]

    def get_table_description(self, cursor, table_name):
        print("get_table_description", table_name)
        return []

    def get_relations(self, cursor, table_name):
        return []
