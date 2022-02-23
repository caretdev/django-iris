class CursorWrapper:
    def __init__(self, cursor):
        self.cursor = cursor

    def _fix_for_params(self, query, params):
        if query.endswith(';'):
            query = query[:-1]
        if params is None:
            params = []
        elif hasattr(params, 'keys'):
            # Handle params as dict
            args = {k: "?" % k for k in params}
            query = query % args
        else:
            # Handle params as sequence
            args = ['?' for i in range(len(params))]
            query = query % tuple(args)
        return query, list(params)

    def execute(self, query, params=None):
        self.times = 0
        query, params = self._fix_for_params(query, params)
        # print(query, params)
        return self.cursor.execute(query, params)

    def executemany(self, query, params=None):
        self.times = 0
        query, params = self._fix_for_params(query, params)
        return self.cursor.executemany(query, params)

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

    def fetchall(self):
        rows = self.cursor.fetchall()
        rows = [tuple(r) for r in rows]
        return rows

    def fetchmany(self, size=None):
        # workaround for endless loop
        if self.times > 0:
            return []
        self.times += 1
        rows = self.cursor.fetchmany(size)
        rows = [tuple(r) for r in rows]
        return rows

    def fetchone(self):
        row = self.cursor.fetchone()
        return tuple(row) if row else None
