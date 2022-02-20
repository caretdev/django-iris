from django.db.backends.base.validation import BaseDatabaseValidation

class DatabaseValidation(BaseDatabaseValidation):
    def check(self, **kwargs):
        return []

    def check_field(self, field, **kwargs):
        return []
