from django.db.backends.base.creation import BaseDatabaseCreation


class DatabaseCreation(BaseDatabaseCreation):

    def _execute_create_test_db(self, cursor, parameters, keepdb=False):
      pass
