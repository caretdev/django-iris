from django.db.models.expressions import Exists

def exists_as_intersystems(self, compiler, connection, template=None, **extra_context):
    template = "(SELECT COUNT(*) FROM (%(subquery)s))"
    return self.as_sql(compiler, connection, template, **extra_context)

Exists.as_intersystems = exists_as_intersystems

