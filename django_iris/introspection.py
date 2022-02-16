from collections import namedtuple

from django.db.backends.base.introspection import (
    BaseDatabaseIntrospection, FieldInfo as BaseFieldInfo, TableInfo,
)
from django.db.models import Index
from django.utils.datastructures import OrderedSet

FieldInfo = namedtuple('FieldInfo', BaseFieldInfo._fields + ('auto_increment', ))

class DatabaseIntrospection(BaseDatabaseIntrospection):
    data_types_reverse = {
        'bigint': 'BigIntegerField',
        'varchar': 'CharField',
        'integer': 'IntegerField',
        'bit': 'BooleanField',
        'date': 'DateField',
        'timestamp': 'DateTimeField',
        'numeric': 'IntegerField',
        'double': 'FloatField',
        'varbinary': 'BinaryField',
        'longvarchar': 'TextField',
        'longvarbinary': 'BinaryField',
        'time': 'TimeField',
        'smallint': 'SmallIntegerField',
        'tinyint': 'SmallIntegerField',
    }

    def get_field_type(self, data_type, field_info):
        field_type = super().get_field_type(data_type, field_info)
        if (field_info.auto_increment and
                field_type in {'BigIntegerField', 'IntegerField', 'SmallIntegerField'}):
            return 'AutoField'
        return field_type

    def get_sequences(self, cursor, table_name, table_fields=()):
        """
        Return a list of introspected sequences for table_name. Each sequence
        is a dict: {'table': <table_name>, 'column': <column_name>}. An optional
        'name' key can be added if the backend supports named sequences.
        """
        for field_info in self.get_table_description(cursor, table_name):
            if 'auto_increment' in field_info and field_info['auto_increment']:
                return [{'table': table_name, 'column': field_info.name}]
        return []

    def get_table_list(self, cursor):
        cursor.execute("""
          SELECT TABLE_NAME
          FROM information_schema.tables
          WHERE TABLE_SCHEMA = 'SQLUser'
        """)
        return [TableInfo(row[0], 't') for row in cursor.fetchall()]

    def get_table_description(self, cursor, table_name):
        """
        Return a description of the table with the DB-API cursor.description
        interface.
        """

        cursor.execute("""
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                IS_NULLABLE,
                AUTO_INCREMENT
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = %s
            AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
        """,
                       ['SQLUser', table_name]
                       )

        description = [
            # name type_code display_size internal_size precision scale null_ok default collation
            # auto_increment
            FieldInfo(
                name,
                data_type,
                None,
                length,
                precision,
                scale,
                isnull == 'YES',
                '',
                '',
                auto_increment == 'YES',
            )
            for name, data_type, length, precision, scale, isnull, auto_increment in cursor.fetchall()
        ]
        return description

    def get_relations(self, cursor, table_name):
        """
        Return a dictionary of {field_name: (field_name_other_table, other_table)}
        representing all relationships to the given table.
        """
        # Dictionary of relations to return
        relations = {}
        return relations

    # def get_primary_key_column(self, cursor, table_name):
    #     """
    #     Return the name of the primary key column for the given table.
    #     """
        # return super().get_primary_key_column(cursor, table_name)

    def get_constraints(self, cursor, table_name):
        """
        Retrieve any constraints or keys (unique, pk, fk, check, index)
        across one or more columns.

        Return a dict mapping constraint names to their attributes,
        where attributes is a dict with keys:
         * columns: List of columns this covers
         * primary_key: True if primary key, False otherwise
         * unique: True if this is a unique constraint, False otherwise
         * foreign_key: (table, column) of target, or None
         * check: True if check constraint, False otherwise
         * index: True if index, False otherwise.
         * orders: The order (ASC/DESC) defined for the columns of indexes
         * type: The type of the index (btree, hash, etc.)

        Some backends may return special constraint names that don't exist
        if they don't name constraints of a certain type (e.g. SQLite)
        """
        constraints = {}

        cursor.execute("""
            SELECT 
                INDEX_NAME, 
                COLUMN_NAME, 
                PRIMARY_KEY,
                NON_UNIQUE,
                ASC_OR_DESC
            FROM INFORMATION_SCHEMA. INDEXES
            WHERE TABLE_SCHEMA = %s
              AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
        """,
                       ['SQLUser', table_name]
                       )
        for index, column, primary, non_unique, order in cursor.fetchall():
            if index not in constraints:
                constraints[index] = {
                    'columns': [],
                    'primary_key': primary == 1,
                    'unique': not non_unique,
                    'check': False,
                    'foreign_key': None,
                    'orders': [],
                }
                constraints[index]['columns'].append(column)
                constraints[index]['orders'].append(
                    'DESC' if order == 'D' else 'ASC')

        return constraints
