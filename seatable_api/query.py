from .grammar import parse_sql


class QuerySet(object):

    def __init__(self, raw_rows, raw_columns, conditions):
        self.rows = raw_rows
        self.raw_rows = raw_rows
        self.raw_columns = raw_columns
        self._execute(conditions)

    def __iter__(self):
        return iter(self.rows)

    def __len__(self):
        return len(self.rows)

    def _execute(self, conditions):
        if conditions:
            self.rows = self.rows

    def filter(self, sql=None):
        conditions = parse_sql(sql)
        if conditions:
            self.rows = self.rows

    def first(self):
        if self.rows:
            return self.rows[0]
        else:
            return None

    def last(self):
        if self.rows:
            return self.rows[-1]
        else:
            return None

    def count(self):
        return len(self.rows)

