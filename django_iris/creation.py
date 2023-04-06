from django.db.backends.base.creation import BaseDatabaseCreation


class DatabaseCreation(BaseDatabaseCreation):

    def _create_test_db(self, verbosity, autoclobber, keepdb=False):
        with self._nodb_cursor() as cursor:
            cursor.execute("""
CREATE OR REPLACE PROCEDURE %ZDJANGO.CLONE_DATABASE(sourceNS %String, targetNS %String)
LANGUAGE OBJECTSCRIPT
{
	new $namespace
	set $namespace = "%SYS"
	
	$$$ThrowOnError(##class(Config.Namespaces).Get(sourceNS, .sourceNSparams))
	$$$ThrowOnError(##class(Config.Namespaces).Get(targetNS, .targetNSparams))
	
	for kind="Globals", "Routines" {
		$$$ThrowOnError(##class(Config.Databases).Get(sourceNSparams(kind), .sourceDBparams))
		$$$ThrowOnError(##class(Config.Databases).Get(targetNSparams(kind), .targetDBparams))
		
		set from = sourceDBparams("Directory")
		set to = targetDBparams("Directory")
		
		quit:$Data(done(to))
		set done(to) = "" 
		
		$$$ThrowOnError(##class(SYS.Database).Copy(from, to, , , 4))
	}
}            """)
        return super()._create_test_db(verbosity, autoclobber, keepdb=keepdb)

    def _clone_test_db(self, suffix, verbosity, keepdb=False):
        source_database_name = self.connection.settings_dict["NAME"]
        target_database_name = self.get_test_db_clone_settings(suffix)["NAME"]

        with self._nodb_cursor() as cursor:
            cursor.execute(f"CREATE DATABASE {target_database_name}")
            cursor.execute(f"CALL %ZDJANGO.CLONE_DATABASE('{source_database_name}', '{target_database_name}')")
        
