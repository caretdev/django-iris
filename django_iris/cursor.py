class CursorWrapper:
    def __init__(self, cursor):
        self.cursor = cursor

    # Django supports only %s params, convert them to ? for IRIS
    def _replace_params(self, query, params_count=0):
        if query.endswith(";"):
            query = query[0:-1]
        # return (query % tuple([f":%qpar({i+1})" for i in range(params_count)])) if params_count > 0 else query.replace('%%', '%')
        return (query % tuple("?" * params_count)) if params_count > 0 else query.replace('%%', '%')

    def execute(self, query, params=None):
        query = self._replace_params(query, len(params) if params else 0)
        return self.cursor.execute(query, params)

    def executemany(self, query, params=None):
        if not isinstance(params, tuple) and not isinstance(params, list):
            params = tuple(params)
        query = self._replace_params(query, len(params[0]) if params else 0)
        try:
            return self.cursor.executemany(query, params)
        except ValueError:
            # Ignore if missing parameters, suppose just insert nothing
            pass

    def close(self):
        try:
            self.cursor.close()
        except:
            # already closed
            pass

    def __getattr__(self, attr):
        return getattr(self.cursor, attr)

    def __iter__(self):
        return iter(self.cursor)
