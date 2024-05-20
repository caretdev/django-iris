from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.base.client import BaseDatabaseClient
from django.db.backends.base.creation import BaseDatabaseCreation
from django.core.exceptions import ImproperlyConfigured
from django.utils.asyncio import async_unsafe
from django.utils.functional import cached_property
from django.db.utils import DatabaseErrorWrapper

from .introspection import DatabaseIntrospection
from .features import DatabaseFeatures
from .schema import DatabaseSchemaEditor
from .operations import DatabaseOperations
from .cursor import CursorWrapper
from .creation import DatabaseCreation
from .validation import DatabaseValidation

import intersystems_iris.dbapi._DBAPI as Database


def ignore(*args, **kwargs):
    pass


class DatabaseClient(BaseDatabaseClient):
    runshell = ignore


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = "intersystems"
    display_name = "InterSystems IRIS"

    data_types = {
        "AutoField": "IDENTITY",
        "BigAutoField": "IDENTITY",
        "BinaryField": "LONG BINARY",
        "BooleanField": "BIT",
        "CharField": "VARCHAR(%(max_length)s)",
        "DateField": "DATE",
        "DateTimeField": "TIMESTAMP",
        "DecimalField": "NUMERIC(%(max_digits)s, %(decimal_places)s)",
        "DurationField": "BIGINT",
        "FileField": "VARCHAR(%(max_length)s)",
        "FilePathField": "VARCHAR(%(max_length)s)",
        "FloatField": "DOUBLE PRECISION",
        "IntegerField": "INTEGER",
        "BigIntegerField": "BIGINT",
        "IPAddressField": "CHAR(15)",
        "GenericIPAddressField": "CHAR(39)",
        "JSONField": "VARCHAR(32768)",
        "OneToOneField": "INTEGER",
        "PositiveBigIntegerField": "BIGINT",
        "PositiveIntegerField": "INTEGER",
        "PositiveSmallIntegerField": "SMALLINT",
        "SlugField": "VARCHAR(%(max_length)s)",
        "SmallAutoField": "IDENTITY",
        "SmallIntegerField": "SMALLINT",
        "TextField": "TEXT",
        "TimeField": "TIME(6)",
        "UUIDField": "UNIQUEIDENTIFIER",
    }

    operators = {
        "exact": "= %s",
        "iexact": "LIKE %s ESCAPE '\\'",
        "contains": "LIKE %s ESCAPE '\\'",
        "icontains": "LIKE %s ESCAPE '\\'",
        # 'regex': "%%%%MATCHES %s ESCAPE '\\'",
        # 'iregex': "%%%%MATCHES %s ESCAPE '\\'",
        "gt": "> %s",
        "gte": ">= %s",
        "lt": "< %s",
        "lte": "<= %s",
        "startswith": "LIKE %s ESCAPE '\\'",
        "endswith": "LIKE %s ESCAPE '\\'",
        "istartswith": "LIKE %s ESCAPE '\\'",
        "iendswith": "LIKE %s ESCAPE '\\'",
    }

    pattern_esc = r"REPLACE(REPLACE(REPLACE({}, '\', '\\'), '%%', '\%%'), '_', '\_')"
    pattern_ops = {
        "contains": "LIKE '%%' || {} || '%%'",
        "icontains": "LIKE '%%' || UPPER({}) || '%%'",
        "startswith": "LIKE {} || '%%'",
        "istartswith": "LIKE UPPER({}) || '%%'",
        "endswith": "LIKE '%%' || {}",
        "iendswith": "LIKE '%%' || UPPER({})",
    }

    Database = Database

    # _commit = ignore
    # _rollback = ignore
    # _savepoint = ignore
    # _savepoint_commit = ignore
    # _savepoint_rollback = ignore
    # _set_autocommit = ignore

    SchemaEditorClass = DatabaseSchemaEditor

    client_class = DatabaseClient
    creation_class = DatabaseCreation
    features_class = DatabaseFeatures
    introspection_class = DatabaseIntrospection
    ops_class = DatabaseOperations
    validation_class = DatabaseValidation

    _disable_constraint_checking = False

    def get_connection_params(self):
        settings_dict = self.settings_dict

        conn_params = {
            "username": None,
            "password": None,
            "timeout": 30,
        }
        if settings_dict["USER"]:
            conn_params["username"] = settings_dict["USER"]
        if settings_dict["PASSWORD"]:
            conn_params["password"] = settings_dict["PASSWORD"]
        if "TIMEOUT" in settings_dict:
            conn_params["timeout"] = settings_dict["TIMEOUT"]
        if "EMBEDDED" in settings_dict:
            conn_params["embedded"] = settings_dict["EMBEDDED"]
        if "CONNECTION_STRING" in settings_dict:
            conn_params["connectionstr"] = settings_dict["CONNECTION_STRING"]
        else:
            conn_params = {
                "hostname": "localhost",
                "port": 1972,
                "namespace": "USER",
                **conn_params,
            }
            if settings_dict["HOST"]:
                conn_params["hostname"] = settings_dict["HOST"]
            if settings_dict["PORT"]:
                conn_params["port"] = settings_dict["PORT"]
            if "NAMESPACE" in settings_dict:
                conn_params["namespace"] = settings_dict["NAMESPACE"]
            if settings_dict["NAME"]:
                conn_params["namespace"] = settings_dict["NAME"]

            if (
                not conn_params["hostname"]
                or not conn_params["port"]
                or not conn_params["namespace"]
            ):
                raise ImproperlyConfigured(
                    "settings.DATABASES is improperly configured. "
                    "Please supply the HOST, PORT and NAME"
                )

        if not conn_params["username"] or not conn_params["password"]:
            raise ImproperlyConfigured(
                "settings.DATABASES is improperly configured. "
                "Please supply the USER and PASSWORD"
            )

        conn_params["application_name"] = "django"
        conn_params["autoCommit"] = self.autocommit
        return conn_params

    def init_connection_state(self):
        pass

    @async_unsafe
    def get_new_connection(self, conn_params):
        return Database.connect(**conn_params)

    def _close(self):
        if self.connection is not None:
            # Automatically rollbacks anyway
            # self.in_atomic_block = False
            # self.needs_rollback = False
            with self.wrap_database_errors:
                return self.connection.close()

    @async_unsafe
    def create_cursor(self, name=None):
        cursor = self.connection.cursor()
        return CursorWrapper(cursor)

    def is_usable(self):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1")
        except:
            return False
        else:
            return True

    @cached_property
    def wrap_database_errors(self):
        """
        Context manager and decorator that re-throws backend-specific database
        exceptions using Django's common wrappers.
        """
        return DatabaseErrorWrapper(self)

    def _set_autocommit(self, autocommit):
        with self.wrap_database_errors:
            self.connection.setAutoCommit(autocommit)

    def disable_constraint_checking(self):
        self._disable_constraint_checking = True
        return True

    def enable_constraint_checking(self):
        self._disable_constraint_checking = False

    @async_unsafe
    def savepoint_commit(self, sid):
        """
        IRIS does not have `RELEASE SAVEPOINT`
        so, just ignore it
        """
