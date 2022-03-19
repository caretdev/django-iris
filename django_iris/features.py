from django.db.backends.base.features import BaseDatabaseFeatures
class DatabaseFeatures(BaseDatabaseFeatures):
    empty_fetchmany_value = []

    requires_literal_defaults = True
    
    supports_transactions = False
    uses_savepoints = False
    has_bulk_insert =  False
    has_native_uuid_field =  False
    supports_timezones = False
    can_clone_databases =  False
    test_db_allows_multiple_connections = False
    supports_unspecified_pk =  False
    can_return_columns_from_insert = False

    supports_column_check_constraints = False
    supports_table_check_constraints = False
    can_introspect_check_constraints = False

    interprets_empty_strings_as_nulls = True
