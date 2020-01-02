import json
import requests


def parse_headers(token):
    return {'Authorization': 'Token ' + str(token),
            'Content-Type': 'application/json',
            }


def parse_server_url(server_url):
    return server_url[:-1] if server_url[-1] == '/' else server_url


def parse_response(response):
    if response.status_code >= 400:
        raise ConnectionError(response.status_code, response.content)
    else:
        data = json.loads(response.content)
        return data


class SeaTableAPI(object):
    """SeaTable API
    """

    def __init__(self, token, server_url):
        self.token = token
        self.server_url = parse_server_url(server_url)
        self.uuid = None
        self.jwt_token = None
        self.headers = None
        self._auth()

    def _auth(self):
        url = self.server_url + '/api/v2.1/dtable/app-access-token/'
        headers = parse_headers(self.token)
        response = requests.get(url, headers=headers)
        data = parse_response(response)

        self.uuid = data.get('dtable_uuid')
        self.jwt_token = data.get('access_token')
        self.headers = parse_headers(self.jwt_token)

    def _row_server_url(self):
        return self.server_url + '/dtable-server/api/v1/dtables/' + self.uuid + '/rows/'

    def load_rows(self, table_name):
        url = self._row_server_url()
        params = {
            'table_name': table_name,
        }
        response = requests.get(url, params=params, headers=self.headers)
        return parse_response(response)

    def append_row(self, table_name, row_data):
        url = self._row_server_url()
        json_data = {
            'table_name': table_name,
            'row': row_data,
        }
        response = requests.post(url, json=json_data, headers=self.headers)
        return parse_response(response)

    def insert_row(self, table_name, row_data, anchor_row_id):
        url = self._row_server_url()
        json_data = {
            'table_name': table_name,
            'row': row_data,
            'anchor_row_id': anchor_row_id,
        }
        response = requests.post(url, json=json_data, headers=self.headers)
        return parse_response(response)

    def update_row(self, table_name, row_id, row_data):
        url = self._row_server_url()
        json_data = {
            'table_name': table_name,
            'row_id': row_id,
            'row': row_data,
        }
        response = requests.put(url, json=json_data, headers=self.headers)
        return parse_response(response)

    def delete_row(self, table_name, row_id):
        url = self._row_server_url()
        json_data = {
            'table_name': table_name,
            'row_id': row_id,
        }
        response = requests.delete(url, json=json_data, headers=self.headers)
        return parse_response(response)
