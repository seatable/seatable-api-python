import json

from .api import SeaTableAPI


class SeaTable(object):
    """SeaTable SDK
    """

    def __init__(self, server_url, token):
        self._seatable_api = SeaTableAPI(token, server_url)
        self._seatable_api.auth()

    def __str__(self):
        return 'SeaTable Object [ %s ]' % self._seatable_api.uuid

    def _check_column(self, row_data):
        for key in list(row_data.keys()):
            if key not in self.current_columns:
                print('\033[0;33mwarning\033[0m column key [ %s ] not in table [ %s ]'
                      % (key, self.current_table_name))

    def load_rows(self, table_name):
        self.current_table_name = table_name
        data = self._seatable_api.load_rows(table_name)
        rows = data.get('rows')

        self.current_columns = []
        if rows:
            self.current_columns = list(rows[0].keys())
            self.current_columns.remove('_id')

        return rows

    def append_row(self, row_data=None):
        if row_data:
            self._check_column(row_data)
        self._seatable_api.append_row(self.current_table_name, row_data)

    def update_row(self, row, row_data):
        self._check_column(row_data)
        self._seatable_api.update_row(self.current_table_name, row.get('_id'), row_data)

    def delete_row(self, row):
        self._seatable_api.delete_row(self.current_table_name, row.get('_id'))
