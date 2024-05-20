from django.db.backends.base.features import BaseDatabaseFeatures
from django.db import InterfaceError
from django.utils.functional import cached_property


class DatabaseFeatures(BaseDatabaseFeatures):
    empty_fetchmany_value = []

    requires_literal_defaults = True

    supports_paramstyle_pyformat = False

    supports_transactions = True
    uses_savepoints = True
    has_bulk_insert = True
    has_native_uuid_field = True
    supports_timezones = False
    has_zoneinfo_database = False
    can_clone_databases = True
    test_db_allows_multiple_connections = False
    supports_unspecified_pk = False
    can_return_columns_from_insert = False

    # Does the backend support NULLS FIRST and NULLS LAST in ORDER BY?
    supports_order_by_nulls_modifier = False

    # Does the backend orders NULLS FIRST by default?
    order_by_nulls_first = False

    # Can it create foreign key constraints inline when adding columns?
    can_create_inline_fk = False

    # Does the backend consider table names with different casing to
    # be equal?
    ignores_table_name_case = True

    # Support for the DISTINCT ON clause
    can_distinct_on_fields = False

    # Can an object have an autoincrement primary key of 0?
    allows_auto_pk_0 = False

    # Can we roll back DDL in a transaction?
    can_rollback_ddl = False

    # Can an index be renamed?
    can_rename_index = False

    # Does it support operations requiring references rename in a transaction?
    supports_atomic_references_rename = False

    # Can we issue more than one ALTER COLUMN clause in an ALTER TABLE?
    supports_combined_alters = False

    truncates_names = True

    supports_column_check_constraints = False
    supports_table_check_constraints = False
    can_introspect_check_constraints = False

    interprets_empty_strings_as_nulls = False

    supports_ignore_conflicts = False

    closed_cursor_error_class = InterfaceError

    # Does the backend support functions in defaults?
    # IRIS, requires ObjectScript there, no way
    supports_expression_defaults = False

    # Does the backend support partial indexes (CREATE INDEX ... WHERE ...)?
    supports_partial_indexes = False
    supports_functions_in_partial_indexes = False
    # Does the backend support covering indexes (CREATE INDEX ... INCLUDE ...)?
    supports_covering_indexes = False
    # Does the backend support indexes on expressions?
    supports_expression_indexes = False
    # Does the backend treat COLLATE as an indexed expression?
    collate_as_index_expression = False

    # Does the backend support window expressions (expression OVER (...))?
    supports_over_clause = True
    supports_frame_range_fixed_distance = False
    only_supports_unbounded_with_preceding_and_following = False

    # Does the backend support JSONField?
    supports_json_field = False
    # Can the backend introspect a JSONField?
    can_introspect_json_field = False
    # Does the backend support primitives in JSONField?
    supports_primitives_in_json_field = False
    # Is there a true datatype for JSON?
    has_native_json_field = False
    # Does the backend use PostgreSQL-style JSON operators like '->'?
    has_json_operators = False
    # Does the backend support __contains and __contained_by lookups for
    # a JSONField?
    supports_json_field_contains = False
    # Does value__d__contains={'f': 'g'} (without a list around the dict) match
    # {'d': [{'f': 'g'}]}?
    json_key_contains_list_matching_requires_list = False
    # Does the backend support JSONObject() database function?
    has_json_object_function = False

    # Does the backend support column collations?
    supports_collation_on_charfield = True
    supports_collation_on_textfield = False

    # Collation names for use by the Django test suite.
    test_collations = {
        # "ci": None,  # Case-insensitive.
        "cs": "EXACT",  # Case-sensitive.
        # "non_default": None,  # Non-default.
        # "swedish_ci": None,  # Swedish case-insensitive.
        "virtual": None
    }

    @cached_property
    def django_test_skips(self):
        skips = super().django_test_skips
        skips.update(
            {
                "IRIS doesn't support Hash functions.": {
                    "db_functions.text.test_md5.MD5Tests.test_basic",
                    "db_functions.text.test_md5.MD5Tests.test_transform",
                    "db_functions.text.test_sha1.SHA1Tests.test_basic",
                    "db_functions.text.test_sha1.SHA1Tests.test_transform",
                    "db_functions.text.test_sha224.SHA224Tests.test_basic",
                    "db_functions.text.test_sha224.SHA224Tests.test_transform",
                    "db_functions.text.test_sha256.SHA256Tests.test_basic",
                    "db_functions.text.test_sha256.SHA256Tests.test_transform",
                    "db_functions.text.test_sha384.SHA384Tests.test_basic",
                    "db_functions.text.test_sha384.SHA384Tests.test_transform",
                    "db_functions.text.test_sha512.SHA512Tests.test_basic",
                    "db_functions.text.test_sha512.SHA512Tests.test_transform",
                },
                "Unexpected result for RPAD/LPAD with 0 as length": {
                    "db_functions.text.test_pad.PadTests.test_pad",
                },
                "IRIS has bugs with renaming": {
                    "schema.tests.SchemaTests.test_alter_pk_with_self_referential_field",
                    "schema.tests.SchemaTests.test_alter_to_fk",
                    "schema.tests.SchemaTests.test_char_field_with_db_index_to_fk",
                    "schema.tests.SchemaTests.test_db_table",
                    "schema.tests.SchemaTests.test_m2m_repoint_custom",
                    "schema.tests.SchemaTests.test_m2m_repoint_inherited",
                    "schema.tests.SchemaTests.test_m2m_repoint",
                    "schema.tests.SchemaTests.test_rename_referenced_field",
                    "schema.tests.SchemaTests.test_text_field_with_db_index_to_fk",
                    "schema.tests.SchemaTests.test_rename_table_renames_deferred_sql_references",
                    "schema.tests.SchemaTests.test_rename_keep_null_status",
                    "schema.tests.SchemaTests.test_rename_column_renames_deferred_sql_references",
                    "schema.tests.SchemaTests.test_rename",
                },
                "IRIS does not care about ASC/DESC in index": {
                    "schema.tests.SchemaTests.test_order_index",
                },
                "Bug on IRIS side, add DATE column with default value on existing data": {
                    "schema.tests.SchemaTests.test_add_datefield_and_datetimefield_use_effective_default",
                },
                # "No idea what's to do here": {
                #     "datetimes.tests.DateTimesTests.test_datetimes_ambiguous_and_invalid_times",
                # },
                "IRIS does not have check contsraints": {
                    "constraints.tests.CheckConstraintTests.test_validate",
                    "constraints.tests.CheckConstraintTests.test_validate_boolean_expressions",
                    "constraints.tests.UniqueConstraintTests.test_validate_expression_condition",
                },
            }

        )
        return skips

    django_test_expected_failures = {
        # IRIS does not support DROP IDENTITY, and changing IDENTITY on fly
        "schema.tests.SchemaTests.test_alter_auto_field_to_char_field",
        "schema.tests.SchemaTests.test_alter_auto_field_to_integer_field",
        "schema.tests.SchemaTests.test_autofield_to_o2o",
        "schema.tests.SchemaTests.test_alter_int_pk_to_autofield_pk",
        "schema.tests.SchemaTests.test_alter_int_pk_to_bigautofield_pk",
        "schema.tests.SchemaTests.test_alter_smallint_pk_to_smallautofield_pk",
        "schema.tests.SchemaTests.test_char_field_pk_to_auto_field",
        # ADD COLUMN does not create unique indexes
        "schema.tests.SchemaTests.test_indexes",
        # Change column datatype to/from TextField (IRIS Streams)
        "schema.tests.SchemaTests.test_alter_text_field_to_time_field",
        "schema.tests.SchemaTests.test_alter_text_field_to_datetime_field",
        "schema.tests.SchemaTests.test_alter_text_field_to_date_field",
    }

    # django_test_skips["IRIS Bugs"] = django_test_expected_failures

    @cached_property
    def introspected_field_types(self):
        return {
            **super().introspected_field_types,
            "BigAutoField": "AutoField",
            "DurationField": "BigIntegerField",
            "GenericIPAddressField": "CharField",
            "SmallAutoField": "AutoField",
        }
