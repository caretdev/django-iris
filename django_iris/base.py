from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.base.client import BaseDatabaseClient
from django.db.backends.base.creation import BaseDatabaseCreation
from django.db.backends.base.introspection import BaseDatabaseIntrospection
from django.db.backends.base.operations import BaseDatabaseOperations

from django.db.backends.base.introspection import (
    BaseDatabaseIntrospection, FieldInfo as BaseFieldInfo, TableInfo,
)

from django.utils.asyncio import async_unsafe


from .introspection import DatabaseIntrospection
from .features import DatabaseFeatures
from .schema import DatabaseSchemaEditor
from .operations import DatabaseOperations


import iris as Database
import traceback


def ignore(*args, **kwargs):
    pass


class DatabaseClient(BaseDatabaseClient):
    runshell = ignore


class DatabaseCreation(BaseDatabaseCreation):
    create_test_db = ignore
    destroy_test_db = ignore


class CursorWrapper:
    def __init__(self, cursor):
        self.cursor = cursor

    def _fix_for_params(self, query, params):
        if query.endswith(';'):
            query = query[:-1]
        if params is None:
            params = []
        elif hasattr(params, 'keys'):
            # Handle params as dict
            args = {k: "?" % k for k in params}
            query = query % args
        else:
            # Handle params as sequence
            args = ['?' for i in range(len(params))]
            query = query % tuple(args)
        return query, list(params)

    def execute(self, query, params=None):
        self.times = 0
        query, params = self._fix_for_params(query, params)
        # print(query, params)
        self.cursor.execute(query, params)

    def executemany(self, query, params=None):
        # print(query, params)
        self.times = 0
        query, params = self._fix_for_params(query, params)
        self.cursor.executemany(query, params)

    def close(self):
        self.cursor.close()

    def __getattr__(self, attr):
        return getattr(self.cursor, attr)

    def __iter__(self):
        return iter(self.cursor)

    def fetchall(self):
        rows = self.cursor.fetchall()
        rows = [tuple(r) for r in rows]
        return rows

    def fetchmany(self, size=None):
        # workaround for endless loop
        if self.times > 0:
            return []
        self.times += 1
        rows = self.cursor.fetchmany(size)
        rows = [tuple(r) for r in rows]
        return rows

    def fetchone(self):
        return tuple(self.cursor.fetchone())


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = 'interystems'
    display_name = 'InterSystems IRIS'

    data_types = {
        'AutoField': 'INTEGER AUTO_INCREMENT',
        'BigAutoField': 'BIGINT AUTO_INCREMENT',
        'BinaryField': 'LONG BINARY',
        'BooleanField': 'BIT',
        'CharField': 'VARCHAR(%(max_length)s)',
        'DateField': 'DATE',
        'DateTimeField': 'TIMESTAMP',
        'DecimalField': 'NUMERIC(%(max_digits)s, %(decimal_places)s)',
        'DurationField': 'BIGINT',
        'FileField': 'VARCHAR(%(max_length)s)',
        'FilePathField': 'VARCHAR(%(max_length)s)',
        'FloatField': 'DOUBLE PRECISION',
        'IntegerField': 'INTEGER',
        'BigIntegerField': 'BIGINT',
        'IPAddressField': 'CHAR(15)',
        'GenericIPAddressField': 'CHAR(39)',
        'JSONField': 'json',
        'OneToOneField': 'INTEGER',
        'PositiveBigIntegerField': 'BIGINT',
        'PositiveIntegerField': 'INTEGER',
        'PositiveSmallIntegerField': 'SMALLINT',
        'SlugField': 'VARCHAR(%(max_length)s)',
        'SmallAutoField': 'SMALLINT AUTO_INCREMENT',
        'SmallIntegerField': 'SMALLINT',
        'TextField': 'LONG',
        'TimeField': 'TIME(6)',
        'UUIDField': 'CHAR(32)',
    }

    operators = {
        'exact': '= %s',
        # 'iexact': "LIKE %s ESCAPE '\\'",
        # 'contains': "LIKE %s ESCAPE '\\'",
        # 'icontains': "LIKE %s ESCAPE '\\'",
        # 'regex': 'REGEXP %s',
        # 'iregex': "REGEXP '(?i)' || %s",
        'gt': '> %s',
        'gte': '>= %s',
        'lt': '< %s',
        'lte': '<= %s',
        'startswith': "%STARTSWITH %s",
        # 'endswith': "LIKE %s ESCAPE '\\'",
        'istartswith': "%STARTSWITH %s",
        # 'iendswith': "LIKE %s ESCAPE '\\'",

    }
    Database = Database

    _commit = ignore
    _rollback = ignore
    _close = ignore
    _savepoint = ignore
    _savepoint_commit = ignore
    _savepoint_rollback = ignore
    _set_autocommit = ignore

    SchemaEditorClass = DatabaseSchemaEditor

    client_class = DatabaseClient
    creation_class = DatabaseCreation
    features_class = DatabaseFeatures
    introspection_class = DatabaseIntrospection
    ops_class = DatabaseOperations

    def get_connection_params(self):
        kwargs = {
            'hostname': 'localhost',
            'port': 1972,
            'namespace': 'USER',
            'username': '_SYSTEM',
            'password': 'SYS'
        }
        return kwargs

    @async_unsafe
    def get_new_connection(self, conn_params):
        # print(Database)
        # print(Database.__dict__)
        return Database.connect(**conn_params)

    def init_connection_state(self):
        assignments = []

    @async_unsafe
    def create_cursor(self, name=None):
        # return self.connection.cursor(factory=CursorWrapper)
        cursor = self.connection.cursor()
        return CursorWrapper(cursor)

    def is_usable(self):
        return True
