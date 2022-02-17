from collections import namedtuple

from django.db.backends.base.introspection import (
    BaseDatabaseIntrospection, FieldInfo as BaseFieldInfo, TableInfo,
)
from django.db.models import Index
from django.utils.datastructures import OrderedSet

FieldInfo = namedtuple(
    'FieldInfo', BaseFieldInfo._fields + ('auto_increment', ))


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
        cursor.execute("""
            SELECT column_name, referenced_column_name, referenced_table_name
            FROM information_schema.key_column_usage
            WHERE table_schema = %s
                AND table_name = %s
                AND referenced_table_name IS NOT NULL
                AND referenced_column_name IS NOT NULL
        """, ['SQLUser', table_name])
        return {
            field_name: (other_field, other_table)
            for field_name, other_field, other_table in cursor.fetchall()
        }

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
        # Get the actual constraint names and columns
        name_query = """
            SELECT kc.constraint_name, kc.column_name,
                kc.referenced_table_name, kc.referenced_column_name,
                c.constraint_type
            FROM
                information_schema.key_column_usage AS kc,
                information_schema.table_constraints AS c
            WHERE
                kc.table_schema = %s AND
                c.table_schema = kc.table_schema AND
                c.constraint_name = kc.constraint_name AND
                c.constraint_type != 'CHECK' AND
                kc.table_name = %s
            ORDER BY kc.ordinal_position
        """
        cursor.execute(name_query, ['SQLUser', table_name])
        for constraint, column, ref_table, ref_column, kind in cursor.fetchall():
            if constraint not in constraints:
                constraints[constraint] = {
                    'columns': OrderedSet(),
                    'primary_key': kind == 'PRIMARY KEY',
                    'unique': kind in {'PRIMARY KEY', 'UNIQUE'},
                    'index': False,
                    'check': False,
                    'foreign_key': (ref_table, ref_column) if ref_column else None,
                }
                if self.connection.features.supports_index_column_ordering:
                    constraints[constraint]['orders'] = []
            constraints[constraint]['columns'].add(column)
        # Add check constraints.
        if self.connection.features.can_introspect_check_constraints:
            unnamed_constraints_index = 0
            columns = {info.name for info in self.get_table_description(
                cursor, table_name)}
            type_query = """
                SELECT cc.constraint_name, cc.check_clause
                FROM
                    information_schema.check_constraints AS cc,
                    information_schema.table_constraints AS tc
                WHERE
                    cc.constraint_schema = %s AND
                    tc.table_schema = cc.constraint_schema AND
                    cc.constraint_name = tc.constraint_name AND
                    tc.constraint_type = 'CHECK' AND
                    tc.table_name = %s
            """
            cursor.execute(type_query, ['SQLUser', table_name])
            for constraint, check_clause in cursor.fetchall():
                constraint_columns = self._parse_constraint_columns(
                    check_clause, columns)
                # Ensure uniqueness of unnamed constraints. Unnamed unique
                # and check columns constraints have the same name as
                # a column.
                if set(constraint_columns) == {constraint}:
                    unnamed_constraints_index += 1
                    constraint = '__unnamed_constraint_%s__' % unnamed_constraints_index
                constraints[constraint] = {
                    'columns': constraint_columns,
                    'primary_key': False,
                    'unique': False,
                    'index': False,
                    'check': True,
                    'foreign_key': None,
                }

        index_query = """
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
        """
        cursor.execute(index_query, ['SQLUser', table_name])
        for index, column, primary, non_unique, order in cursor.fetchall():
            if index not in constraints:
                constraints[index] = {
                    'columns': OrderedSet(),
                    'primary_key': primary == 1,
                    'unique': not non_unique,
                    'check': False,
                    'foreign_key': None,
                    'orders': [],
                }
                constraints[index]['columns'].add(column)
                constraints[index]['orders'].append(
                    'DESC' if order == 'D' else 'ASC')

        # Convert the sorted sets to lists
        for constraint in constraints.values():
            constraint['columns'] = list(constraint['columns'])
        return constraints
