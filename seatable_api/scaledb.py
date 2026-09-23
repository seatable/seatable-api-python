import requests

from .exception import ScaleDBAPIError
from .utils import parse_headers, parse_server_url


def check_auth(func):

    def wrapper(obj, *args, **kwargs):
        if not obj.is_authed:
            raise ScaleDBAPIError('ScaleDB has not been authenticated, call auth() first.')
        return func(obj, *args, **kwargs)
    return wrapper


def parse_seadb_response(response):
    try:
        raw = response.json()
    except ValueError:
        raw = None
    data = raw if isinstance(raw, dict) else {}
    error = data.get('error_message') or data.get('error_msg')
    if response.status_code >= 400 or response.status_code < 200:
        raise ScaleDBAPIError(error or '%s %s' % (response.status_code, response.text), response.status_code)
    if data.get('success') is False or error:
        raise ScaleDBAPIError(error or 'request failed.', response.status_code)
    if not isinstance(raw, dict):
        raise ScaleDBAPIError('invalid response: %s' % response.text, response.status_code)
    return data


class ScaleDB(object):
    """ScaleDB API
    """

    def __init__(self, token, server_url):
        """
        :param token: str
        :param server_url: str
        """
        self.token = token
        self.server_url = parse_server_url(server_url)
        self.seadb_server_url = None
        self.jwt_token = None
        self.headers = None
        self.database_uuid = None
        self.database_name = None
        self.permission = None
        self.timeout = 30
        self.is_authed = False

    def __str__(self):
        return '<ScaleDB Database [ %s ]>' % self.database_name

    def auth(self):
        """Auth to ScaleDB
        """
        url = self.server_url + '/api/v2.1/seadb/app-access-token/'
        headers = parse_headers(self.token)
        response = requests.get(url, headers=headers, timeout=self.timeout)
        data = parse_seadb_response(response)

        database_uuid = data.get('database_uuid')
        seadb_server = data.get('seadb_server')
        access_token = data.get('access_token')
        if not database_uuid or not seadb_server or not access_token:
            raise ScaleDBAPIError('auth response is incomplete.')

        self.seadb_server_url = parse_server_url(seadb_server)
        self.jwt_token = access_token
        self.headers = {
            'Authorization': 'Bearer ' + self.jwt_token,
            'Content-Type': 'application/json',
        }
        self.database_uuid = database_uuid
        self.database_name = data.get('database_name')
        self.permission = data.get('permission')
        self.is_authed = True

    def _query_url(self):
        return self.seadb_server_url + '/api/v1/' + self.database_uuid + '/query'

    def _rows_url(self):
        return self.seadb_server_url + '/api/v1/' + self.database_uuid + '/rows'

    def _tables_url(self):
        return self.seadb_server_url + '/api/v1/' + self.database_uuid + '/tables'

    def _columns_url(self):
        return self.seadb_server_url + '/api/v1/' + self.database_uuid + '/columns'

    def _column_options_url(self):
        return self.seadb_server_url + '/api/v1/' + self.database_uuid + '/column-options'

    @check_auth
    def add_table(self, table_name):
        """
        :param table_name: str
        :return: str, table id
        """
        url = self._tables_url()
        json_data = {
            'table_name': table_name,
        }
        response = requests.post(url, json=json_data, headers=self.headers, timeout=self.timeout)
        data = parse_seadb_response(response)
        return data.get('table_id')

    @check_auth
    def rename_table(self, table_name, new_table_name):
        """
        :param table_name: str
        :param new_table_name: str
        :return: dict
        """
        url = self._tables_url()
        json_data = {
            'table_name': table_name,
            'new_table_name': new_table_name,
        }
        response = requests.put(url, json=json_data, headers=self.headers, timeout=self.timeout)
        return parse_seadb_response(response)

    @check_auth
    def delete_table(self, table_name):
        """
        :param table_name: str
        :return: dict
        """
        url = self._tables_url()
        json_data = {
            'table_name': table_name,
        }
        response = requests.delete(url, json=json_data, headers=self.headers, timeout=self.timeout)
        return parse_seadb_response(response)

    @check_auth
    def insert_column(self, table_name, column_name, column_type, column_data=None):
        """
        :param table_name: str
        :param column_name: str
        :param column_type: str, text/int64/float64/float32/bool/datetime/single-select/multiple-select/list
        :param column_data: dict
        :return: str, column key
        """
        url = self._columns_url()
        json_data = {
            'table_name': table_name,
            'column_name': column_name,
            'column_type': column_type,
        }
        if column_data:
            json_data['column_data'] = column_data
        response = requests.post(url, json=json_data, headers=self.headers, timeout=self.timeout)
        data = parse_seadb_response(response)
        return data.get('column_key')

    @check_auth
    def rename_column(self, table_name, column_name, new_column_name):
        """
        :param table_name: str
        :param column_name: str
        :param new_column_name: str
        :return: dict
        """
        url = self._columns_url()
        json_data = {
            'table_name': table_name,
            'column_name': column_name,
            'new_column_name': new_column_name,
        }
        response = requests.put(url, json=json_data, headers=self.headers, timeout=self.timeout)
        return parse_seadb_response(response)

    @check_auth
    def delete_column(self, table_name, column_name):
        """
        :param table_name: str
        :param column_name: str
        :return: dict
        """
        url = self._columns_url()
        json_data = {
            'table_name': table_name,
            'column_name': column_name,
        }
        response = requests.delete(url, json=json_data, headers=self.headers, timeout=self.timeout)
        return parse_seadb_response(response)

    @check_auth
    def add_column_option(self, table_name, column_name, option_name, option_data=None):
        """
        :param table_name: str
        :param column_name: str
        :param option_name: str
        :param option_data: dict
        :return: str, option id
        """
        url = self._column_options_url()
        json_data = {
            'table_name': table_name,
            'column_name': column_name,
            'option_name': option_name,
        }
        if option_data:
            json_data['option_data'] = option_data
        response = requests.post(url, json=json_data, headers=self.headers, timeout=self.timeout)
        data = parse_seadb_response(response)
        return data.get('option_id')

    @check_auth
    def query(self, sql, convert=True, parameters=None):
        """
        :param sql: str
        :param convert: bool
        :param parameters: list
        :return: list
        """
        if not sql:
            raise ValueError('sql can not be empty.')
        url = self._query_url()
        json_data = {
            'sql': sql,
            'parameters': parameters or [],
            'convert_keys': bool(convert),
        }
        response = requests.post(url, json=json_data, headers=self.headers, timeout=self.timeout)
        data = parse_seadb_response(response)
        return data.get('results') or []

    @check_auth
    def insert_rows(self, table_name, rows, replace=False):
        """
        :param table_name: str
        :param rows: list
        :param replace: bool
        :return: dict
        """
        url = self._rows_url()
        json_data = {
            'table_name': table_name,
            'rows': rows,
            'replace': replace,
        }
        response = requests.post(url, json=json_data, headers=self.headers, timeout=self.timeout)
        return parse_seadb_response(response)

    @check_auth
    def update_rows(self, table_name, updates):
        """
        :param table_name: str
        :param updates: list, [{'pk': int, 'row': dict}]
        :return: dict
        """
        url = self._rows_url()
        json_data = {
            'table_name': table_name,
            'updates': updates,
        }
        response = requests.put(url, json=json_data, headers=self.headers, timeout=self.timeout)
        return parse_seadb_response(response)

    @check_auth
    def delete_rows(self, table_name, pks):
        """
        :param table_name: str
        :param pks: list
        :return: dict
        """
        url = self._rows_url()
        json_data = {
            'table_name': table_name,
            'pks': pks,
        }
        response = requests.delete(url, json=json_data, headers=self.headers, timeout=self.timeout)
        return parse_seadb_response(response)
