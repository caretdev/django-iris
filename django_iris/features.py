from django.db.backends.base.features import BaseDatabaseFeatures
class DatabaseFeatures(BaseDatabaseFeatures):
    empty_fetchmany_value = []
    
    supports_transactions = False
    uses_savepoints = False
    has_bulk_insert =  False
    has_native_uuid_field =  False
    supports_timezones = False
    uses_savepoints = False
    can_clone_databases =  False
    test_db_allows_multiple_connections = False
    supports_unspecified_pk =  False
    can_return_columns_from_insert = False
