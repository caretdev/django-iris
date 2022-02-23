from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.base.client import BaseDatabaseClient
from django.db.backends.base.creation import BaseDatabaseCreation
from django.core.exceptions import ImproperlyConfigured
from django.utils.asyncio import async_unsafe

from .introspection import DatabaseIntrospection
from .features import DatabaseFeatures
from .schema import DatabaseSchemaEditor
from .operations import DatabaseOperations
from .cursor import CursorWrapper
from .creation import DatabaseCreation
from .validation import DatabaseValidation

import iris as Database


Database.Warning = type("StandardError", (object,), {})
Database.Error = type("StandardError", (object,), {})

Database.InterfaceError = type("Error", (object,), {})

Database.DatabaseError = type("Error", (object,), {})
Database.DataError = type("DatabaseError", (object,), {})
Database.OperationalError = type("DatabaseError", (object,), {})
Database.IntegrityError = type("DatabaseError", (object,), {})
Database.InternalError = type("DatabaseError", (object,), {})
Database.ProgrammingError = type("DatabaseError", (object,), {})
Database.NotSupportedError = type("DatabaseError", (object,), {})


def ignore(*args, **kwargs):
    pass


class DatabaseClient(BaseDatabaseClient):
    runshell = ignore


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = 'intersystems'
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
        'JSONField': 'VARCHAR(32768)',
        'OneToOneField': 'INTEGER',
        'PositiveBigIntegerField': 'BIGINT',
        'PositiveIntegerField': 'INTEGER',
        'PositiveSmallIntegerField': 'SMALLINT',
        'SlugField': 'VARCHAR(%(max_length)s)',
        'SmallAutoField': 'SMALLINT AUTO_INCREMENT',
        'SmallIntegerField': 'SMALLINT',
        # 'TextField': 'LONG',
        # Stream not supported yet by db-api
        'TextField': 'VARCHAR(255)',
        'TimeField': 'TIME(6)',
        'UUIDField': 'CHAR(32)',
    }

    operators = {
        'exact': '= %s',
        'iexact': "LIKE %s ESCAPE '\\'",
        'contains': "LIKE %s ESCAPE '\\'",
        'icontains': "LIKE %s ESCAPE '\\'",
        # 'regex': 'REGEXP %s',
        # 'iregex': "REGEXP '(?i)' || %s",
        'gt': '> %s',
        'gte': '>= %s',
        'lt': '< %s',
        'lte': '<= %s',
        'startswith': "%STARTSWITH %s",
        'endswith': "LIKE %s ESCAPE '\\'",
        'istartswith': "%STARTSWITH %s",
        'iendswith': "LIKE %s ESCAPE '\\'",

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
    validation_class = DatabaseValidation

    def get_connection_params(self):
        settings_dict = self.settings_dict

        conn_params = {
            'username': None,
            'password': None,
        }
        if settings_dict['USER']:
            conn_params['username'] = settings_dict['USER']
        if settings_dict['PASSWORD']:
            conn_params['password'] = settings_dict['PASSWORD']

        if 'CONNECTION_STRING' in settings_dict:
            conn_params['connectionstr'] = settings_dict['CONNECTION_STRING']
        else:
            conn_params = {
                'hostname': 'localhost',
                'port': 1972,
                'namespace': 'USER',
                **conn_params,
            }
            if settings_dict['HOST']:
                conn_params['hostname'] = settings_dict['HOST']
            if settings_dict['PORT']:
                conn_params['port'] = settings_dict['PORT']
            if settings_dict['NAME']:
                conn_params['namespace'] = settings_dict['NAME']
            if 'NAMESPACE' in settings_dict:
                conn_params['namespace'] = settings_dict['NAMESPACE']

            if (
                not conn_params['hostname'] or
                not conn_params['port'] or
                not conn_params['namespace']
            ):
                raise ImproperlyConfigured(
                    "settings.DATABASES is improperly configured. "
                    "Please supply the HOST, PORT and NAME"
                )

        if (
            not conn_params['username'] or
            not conn_params['password']
        ):
            raise ImproperlyConfigured(
                "settings.DATABASES is improperly configured. "
                "Please supply the USER and PASSWORD"
            )

        return conn_params

    @async_unsafe
    def get_new_connection(self, conn_params):
        return Database.connect(**conn_params)

    def init_connection_state(self):
        cursor = self.connection.cursor()
        # cursor.callproc('%SYSTEM_SQL.Util_SetOption', ['SELECTMODE', 1])
        # cursor.callproc('%SYSTEM.SQL_SetSelectMode', [1])

    @async_unsafe
    def create_cursor(self, name=None):
        cursor = self.connection.cursor()
        return CursorWrapper(cursor)

    def is_usable(self):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute('SELECT 1')
        except:
            return False
        else:
            return True
