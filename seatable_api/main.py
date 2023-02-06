import io
import json
import re
from datetime import datetime, timedelta
from urllib import parse
from uuid import UUID

# https://requests.readthedocs.io
import requests

from seatable_api.exception import AuthExpiredError
from seatable_api.message import get_sender_by_account
from .constants import ROW_FILTER_KEYS, ColumnTypes
from .constants import RENAME_COLUMN, RESIZE_COLUMN, FREEZE_COLUMN, MOVE_COLUMN, MODIFY_COLUMN_TYPE, DELETE_COLUMN
from .socket_io import SocketIO
from .query import QuerySet
from .utils import convert_db_rows


def parse_server_url(server_url):
    return server_url.rstrip('/')

def parse_response(response):
    if response.status_code >= 400:
        try:
            err_data = json.loads(response.text)
        except:
            err_data = {}
        if err_data.get("error_msg") == "Token expired." and \
            response.status_code == 403:
            raise AuthExpiredError

        raise ConnectionError(response.status_code, response.text)
    else:
        try:
            data = json.loads(response.text)
            return data
        except:
            pass

def like_table_id(value):
    return re.match(r'^[-0-9a-zA-Z]{4}$', value)


class ApiBase(object):
    def __init__(self, session, timeout, ssl_verify):
        #: Create a session object for requests
        self.session = session or requests.Session()
        self.timeout = timeout
        self.ssl_verify = ssl_verify
        self.headers = {}
        self.jwt_token = None
        self.jwt_exp = None

    def _get_session_opts(self):
        return {
            "headers": self.headers.copy(),
            "timeout": self.timeout,
            "verify": self.ssl_verify,
        }

    def token_headers(self, token=None):
        if not token:
            token = self.token
        return {
            'Authorization': 'Token %s' % (token,)
        }

    def json_headers(self, token=None):
        return {
            **self.token_headers(token), **{
            'Content-Type': 'application/json',
            }
        }

    def http_request(self, verb, url, headers={}, timeout=None, params={}, json={}, data={}):
        """Make an HTTP request to the Seatable server"""
        opts = self._get_session_opts()
        verify = opts.pop("verify")
        if timeout is None:
            timeout = opts.pop("timeout")
        headers = {**opts.pop("headers"), **headers}
        result = self.session.request(
            method=verb,
            url=url,
            json=json,
            params=params,
            data=data,
            timeout=timeout,
            verify=verify,
            headers=headers,
            **opts,
        )
        return result


class SeaTableAPI(ApiBase):
    """SeaTable API
    """

    def __init__(self, token, server_url, session=None, ssl_verify=True, timeout=30):
        """
        :param token: str
        :param server_url: str
        :param session: requests.Session()
        :param ssl_verify: bool
        :param timeout: int
        """
        self.token = token
        self.server_url = parse_server_url(server_url)
        self.dtable_server_url = None
        self.dtable_db_url = None
        self.workspace_id = None
        self.dtable_uuid = None
        self.dtable_name = None
        self.socketIO = None
        super().__init__(session, timeout, ssl_verify)


    def __str__(self):
        return '<SeaTable Base [ %s ]>' % self.dtable_name

    def _clone(self):
        clone = self.__class__(self.token, self.server_url)
        clone.dtable_server_url = self.dtable_server_url
        clone.dtable_db_url = self.dtable_db_url
        clone.jwt_token = self.jwt_token
        clone.jwt_exp = self.jwt_exp
        clone.headers = self.headers
        clone.ssl_verify = self.ssl_verify
        clone.workspace_id = self.workspace_id
        clone.dtable_uuid = self.dtable_uuid
        clone.dtable_name = self.dtable_name
        clone.timeout = self.timeout
        return clone

    def _get_msg_sender_by_account(self, account_name):
        try:
            account = self._get_account_detail(account_name)
            msg_sender = get_sender_by_account(account)
        except:
            msg_sender = None
        return msg_sender

    def auth(self, with_socket_io=False):
        """Auth to SeaTable
        """
        self.jwt_exp = datetime.now() + timedelta(days=3)
        url = self.server_url + '/api/v2.1/dtable/app-access-token/'
        response = self.http_request('get', url, headers=self.json_headers())
        data = parse_response(response)

        self.dtable_server_url = parse_server_url(data.get('dtable_server'))
        self.dtable_db_url = parse_server_url(data.get('dtable_db', ''))
        self.jwt_token = data.get('access_token')
        self.headers = self.json_headers(self.jwt_token)
        self.workspace_id = data.get('workspace_id')
        self.dtable_uuid = data.get('dtable_uuid')
        self.dtable_name = data.get('dtable_name')

        if with_socket_io is True:
            base = self._clone()
            self.socketIO = SocketIO(base)
            self.socketIO._connect()

    def _metadata_server_url(self):
        return self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/metadata/'

    def _table_server_url(self):
        return self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/tables/'

    def _row_server_url(self):
        return self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/rows/'

    def _batch_row_server_url(self):
        return self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/batch-append-rows/'

    def _batch_update_row_server_url(self):
        return self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/batch-update-rows/'

    def _batch_delete_row_server_url(self):
        return self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/batch-delete-rows/'

    def _filtered_rows_server_url(self):
        return self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/filtered-rows/'

    def _row_link_server_url(self):
        return self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/links/'

    def _batch_update_row_link_server_url(self):
        return self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/batch-update-links/'

    def _app_download_link_url(self):
        return self.server_url + '/api/v2.1/dtable/app-download-link/'

    def _app_upload_link_url(self):
        return self.server_url + '/api/v2.1/dtable/app-upload-link/'

    def _column_server_url(self):
        return self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/columns/'

    def _column_options_server_url(self):
        return self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/column-options/'

    def _column_cascade_setting_server_url(self):
        return self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/column-cascade-settings/'

    def _third_party_accounts_url(self):
        return self.server_url + '/api/v2.1/dtable/third-party-account/'

    def _dtable_db_query_url(self):
        return self.dtable_db_url + '/api/v1/query/' + self.dtable_uuid + '/'

    def _dtable_db_linked_records_url(self):
        return self.dtable_db_url + '/api/v1/linked-records/' + self.dtable_uuid + '/'

    def _dtable_db_insert_rows_url(self):
        return self.dtable_db_url + '/api/v1/insert-rows/' + self.dtable_uuid + '/'

    def _get_related_users_url(self):
        return '%(server_url)s/api/v2.1/dtables/%(dtable_uuid)s/related-users/' % {
            'server_url': self.server_url,
            'dtable_uuid': self.dtable_uuid
        }

    def _send_toast_notification_url(self):
        return '%(dtable_server_url)s/api/v1/dtables/%(dtable_uuid)s/ui-toasts/' % {
            'dtable_server_url': self.dtable_server_url,
            'dtable_uuid': self.dtable_uuid
        }

    def _add_workflow_task_url(self, token):
        return '%(server_url)s/api/v2.1/workflows/%(token)s/external-task-submit/' % {
            'server_url': self.server_url,
            'token': token
        }

    def _get_account_detail(self, account_name):
        url = self._third_party_accounts_url()
        params = {
            'account_name': account_name
        }
        response = self.http_request('get', url, params=params, headers=self.json_headers())

        data = parse_response(response)
        return data.get('account')

    def send_email(self, account_name, msg,  **kwargs):
        msg_sender = self._get_msg_sender_by_account(account_name)
        if not msg_sender or msg_sender.msg_type != 'email':
            raise ValueError('Email message sender does not configered.')
        msg_sender.send_msg(msg, **kwargs)

    def send_wechat_msg(self, account_name, msg):
        msg_sender = self._get_msg_sender_by_account(account_name)
        if not msg_sender or msg_sender.msg_type != 'wechat':
            raise ValueError('Wechat message sender does not configered.')
        msg_sender.send_msg(msg)

    def get_metadata(self):
        """
        :return: dict
        """
        url = self._metadata_server_url()
        response = self.http_request('get', url)
        data = parse_response(response)
        return data.get('metadata')

    def add_table(self, table_name, lang='en', columns=[]):
        """
        :param table_name: str
        :param lang: str, currently 'en' for English, and 'zh-cn' for Chinese
        :param columns: list
        """
        url = self._table_server_url()
        json_data = {
            'table_name': table_name,
            'lang': lang,
        }
        if columns:
            json_data['columns'] = columns
        response = self.http_request('post', url, json=json_data)
        return parse_response(response)

    def list_rows(self, table_name, view_name=None, order_by=None, desc=False, start=None, limit=None):
        """
        :param table_name: str
        :param view_name: str
        :param order_by: str
        :param desc: boolean
        :param start: int
        :param limit: int
        :return: list
        """
        url = self._row_server_url()
        params = {
            'table_name': table_name
        }
        if like_table_id(table_name):
            params['table_id'] = table_name
        if view_name:
            params['view_name'] = view_name
        if order_by:
            params['order_by'] = order_by
            params['direction'] = 'desc' if desc else 'asc'
        if start:
            params['start'] = start
        if limit:
            params['limit'] = limit
        response = self.http_request('get', url, params=params)
        data = parse_response(response)
        return data.get('rows')

    def get_row(self, table_name, row_id):
        """
        :param table_name: str
        :param row_id: str
        :return: dict
        """
        url = self._row_server_url() + row_id + '/'
        params = {
            'table_name': table_name
        }
        if like_table_id(table_name):
            params['table_id'] = table_name
        response = self.http_request('get', url, params=params)
        data = parse_response(response)
        return data

    def append_row(self, table_name, row_data):
        """
        :param table_name: str
        :param row_data: dict
        """
        url = self._row_server_url()
        json_data = {
            'table_name': table_name,
            'row': row_data,
        }
        if like_table_id(table_name):
            json_data['table_id'] = table_name
        response = self.http_request('post', url, json=json_data)
        return parse_response(response)

    def batch_append_rows(self, table_name, rows_data):
        """
        :param table_name: str
        :param rows_data: dict
        """
        url = self._batch_row_server_url()
        json_data = {
            'table_name': table_name,
            'rows': rows_data,
        }
        if like_table_id(table_name):
            json_data['table_id'] = table_name
        response = self.http_request('post', url, json=json_data)
        return parse_response(response)

    def insert_row(self, table_name, row_data, anchor_row_id):
        """
        :param table_name: str
        :param row_data: dict
        :param anchor_row_id: str
        """
        url = self._row_server_url()
        json_data = {
            'table_name': table_name,
            'row': row_data,
            'anchor_row_id': anchor_row_id,
        }
        if like_table_id(table_name):
            json_data['table_id'] = table_name
        response = self.http_request('post', url, json=json_data)
        return parse_response(response)

    def update_row(self, table_name, row_id, row_data):
        """
        :param table_name: str
        :param row_id: str
        :param row_data: dict
        """
        url = self._row_server_url()
        json_data = {
            'table_name': table_name,
            'row_id': row_id,
            'row': row_data,
        }
        if like_table_id(table_name):
            json_data['table_id'] = table_name
        response = self.http_request('put', url, json=json_data )
        return parse_response(response)

    def batch_update_rows(self, table_name, rows_data):
        """
        :param table_name: str
        :param rows_data: list
        :return:
        """
        url = self._batch_update_row_server_url()
        json_data = {
            'table_name': table_name,
            'updates': rows_data,
        }
        if like_table_id(table_name):
            json_data['table_id'] = table_name
        response = self.http_request('put', url, json=json_data)
        return parse_response(response)

    def delete_row(self, table_name, row_id):
        """
        :param table_name: str
        :param row_id: str
        """
        url = self._row_server_url()
        json_data = {
            'table_name': table_name,
            'row_id': row_id,
        }
        if like_table_id(table_name):
            json_data['table_id'] = table_name
        response = self.http_request('delete', url, json=json_data)
        return parse_response(response)

    def batch_delete_rows(self, table_name, row_ids):
        """
        :param table_name: str
        :param row_ids: list
        """
        url = self._batch_delete_row_server_url()
        json_data = {
            'table_name': table_name,
            'row_ids': row_ids,
        }
        if like_table_id(table_name):
            json_data['table_id'] = table_name
        response = self.http_request('delete', url, json=json_data)
        return parse_response(response)

    def filter_rows(self, table_name, filters, view_name=None, filter_conjunction='And'):
        """
        :param table_name: str
        :param view_name: str
        :param filters: list
        :param filter_conjunction: str, 'And' or 'Or'
        :return: list
        """
        # params check
        if not filters:
            raise ValueError('filters can not be empty.')
        if not isinstance(filters, list):
            raise ValueError('filters invalid.')
        if len(filters) != len([f for f in filters if isinstance(f, dict)]):
            raise ValueError('filters invalid.')

        for f in filters:
            for key in f.keys():
                if key not in ROW_FILTER_KEYS:
                    raise ValueError('filters invalid.')

        if filter_conjunction not in ['And', 'Or']:
            raise ValueError('filter_conjunction invalid, filter_conjunction must be '
                             '"And" or "Or"')

        params = {
            'table_name': table_name,
        }
        if view_name:
            params['view_name'] = view_name

        json_data = {
            'filters': filters,
            'filter_conjunction': filter_conjunction,
        }

        url = self._filtered_rows_server_url()
        response = self.http_request('get', url, json=json_data, params=params)
        data = parse_response(response)
        return data.get('rows')

    def get_file_download_link(self, path):
        """
        :param path: str
        :return: str
        """
        url = self._app_download_link_url()
        params = {'path': path}
        response = self.http_request('get', url, params=params, headers=self.json_headers())
        data = parse_response(response)
        return data.get('download_link')

    def get_file_upload_link(self):
        """
        :return: dict
        """
        url = self._app_upload_link_url()
        response = self.http_request('get', url, headers=self.json_headers())
        data = parse_response(response)
        return data

    def add_link(self, link_id, table_name, other_table_name, row_id, other_row_id):
        """
        :param link_id: str
        :param table_name: str
        :param other_table_name: str
        :param row_id: str
        :param other_row_id: str
        """
        url = self._row_link_server_url()
        json_data = {
            'link_id': link_id,
            'table_name': table_name,
            'other_table_name': other_table_name,
            'table_row_id': row_id,
            'other_table_row_id': other_row_id,
        }
        if like_table_id(table_name):
            json_data['table_id'] = table_name
        if like_table_id(other_table_name):
            json_data['other_table_id'] = other_table_name
        response = self.http_request('post', url, json=json_data)
        return parse_response(response)

    def remove_link(self, link_id, table_name, other_table_name, row_id, other_row_id):
        """
        :param link_id: str
        :param table_name: str
        :param other_table_name: str
        :param row_id: str
        :param other_row_id: str
        """
        url = self._row_link_server_url()
        json_data = {
            'link_id': link_id,
            'table_name': table_name,
            'other_table_name': other_table_name,
            'table_row_id': row_id,
            'other_table_row_id': other_row_id,
        }
        if like_table_id(table_name):
            json_data['table_id'] = table_name
        if like_table_id(other_table_name):
            json_data['other_table_id'] = other_table_name
        response = self.http_request('delete', url, json=json_data )
        return parse_response(response)

    def update_link(self, link_id, table_name, other_table_name, row_id, other_rows_ids):
        """
        :param link_id: str
        :param table_id: str
        :param other_table_id: str
        :param row_id: str
        :param other_rows_ids: list
        """
        if not isinstance(other_rows_ids, list):
            raise ValueError('params other_rows_ids requires type list')
        url = self._row_link_server_url()
        json_data = {
            'link_id': link_id,
            'table_name': table_name,
            'other_table_name': other_table_name,
            'row_id': row_id,
            'other_rows_ids': other_rows_ids,
        }
        if like_table_id(table_name):
            json_data['table_id'] = table_name
        if like_table_id(other_table_name):
            json_data['other_table_id'] = other_table_name
        response = self.http_request('put', url, json=json_data)
        return parse_response(response)

    def batch_update_links(self, link_id, table_name, other_table_name, row_id_list, other_rows_ids_map):
        """
        :param link_id: str
        :param table_id: str
        :param other_table_id: str
        :param row_id_list: []
        :param other_rows_ids_map: dict
        """
        url = self._batch_update_row_link_server_url()
        json_data = {
            'link_id': link_id,
            'table_name': table_name,
            'other_table_name': other_table_name,

            'row_id_list': row_id_list,
            'other_rows_ids_map': other_rows_ids_map,
        }
        if like_table_id(table_name):
            json_data['table_id'] = table_name
        if like_table_id(other_table_name):
            json_data['other_table_id'] = other_table_name
        response = self.http_request('put', url, json=json_data)
        return parse_response(response)

    def get_linked_records(self, table_id, link_column_key, rows):
        """
        :param table_id:  str
        :param link_column_key: str
        :param rows: list
        """
        url = self._dtable_db_linked_records_url()
        json_data = {
            'table_id': table_id,
            'link_column': link_column_key,
            'rows': rows,
        }
        response = self.http_request('post', url, json=json_data)
        return parse_response(response)

    def list_columns(self, table_name, view_name=None):
        """
        :param table_name: str
        :param view_name: str
        :return: list
        """
        url = self._column_server_url()
        params = {
            'table_name': table_name,
        }
        if like_table_id(table_name):
            params['table_id'] = table_name
        if view_name:
            params['view_name'] = view_name
        response = self.http_request('get', url, params=params)
        data = parse_response(response)
        return data.get('columns')

    def get_column_link_id(self, table_name, column_name, view_name=None):
        columns = self.list_columns(table_name, view_name)
        for column in columns:
            if column.get('name') == column_name and column.get('type') == 'link':
                return column.get('data', {}).get('link_id')
        raise ValueError('link type column "%s" does not exist in current view' % column_name)

    def insert_column(self, table_name, column_name, column_type, column_key=None, column_data=None):
        """
        :param table_name: str
        :param column_name: str
        :param column_type: ColumnType enum
        :param column_key: str, which you want to insert after
        :param column_data: dict, config information of column
        :return: dict
        """
        if column_type not in ColumnTypes:
            raise ValueError("type %s invalid!" % (column_type,))
        url = self._column_server_url()
        json_data = {
            'table_name': table_name,
            'column_name': column_name,
            'column_type': column_type.value
        }
        if like_table_id(table_name):
            json_data['table_id'] = table_name
        if column_key:
            json_data['anchor_column'] = column_key
        if column_data:
            json_data['column_data'] = column_data
        response = self.http_request('post', url, json=json_data)
        data = parse_response(response)
        return data

    def rename_column(self, table_name, column_key, new_column_name):
        """
        :param table_name: str
        :param column_key: str
        :param new_column_name: str
        :return: dict
        """
        url = self._column_server_url()
        json_data = {
            'op_type': RENAME_COLUMN,
            'table_name': table_name,
            'column': column_key,
            'new_column_name': new_column_name
        }
        if like_table_id(table_name):
            json_data['table_id'] = table_name
        response = self.http_request('put', url, json=json_data)
        data = parse_response(response)
        return data

    def resize_column(self, table_name, column_key, new_column_width):
        """
        :param table_name: str
        :param column_key: str
        :param old_column_width: int
        :param new_column_width: int
        :return: dict
        """
        url = self._column_server_url()
        json_data = {
            'op_type': RESIZE_COLUMN,
            'table_name': table_name,
            'column': column_key,
            'new_column_width': new_column_width
        }
        if like_table_id(table_name):
            json_data['table_id'] = table_name
        response = self.http_request('put', url, json=json_data)
        data = parse_response(response)
        return data

    def freeze_column(self, table_name, column_key, frozen):
        """
        :param table_name: str
        :param column_key: str
        :param frozen: bool
        :return: dict
        """
        url = self._column_server_url()
        json_data = {
            'op_type': FREEZE_COLUMN,
            'table_name': table_name,
            'column': column_key,
            'frozen': frozen
        }
        if like_table_id(table_name):
            json_data['table_id'] = table_name
        response = self.http_request('put', url, json=json_data)
        data = parse_response(response)
        return data

    def move_column(self, table_name, column_key, target_column_key):
        """
        :param table_name: str
        :param column_key: str
        :param target_column_key: bool
        :return: dict
        """
        url = self._column_server_url()
        json_data = {
            'op_type': MOVE_COLUMN,
            'table_name': table_name,
            'column': column_key,
            'target_column': target_column_key
        }
        if like_table_id(table_name):
            json_data['table_id'] = table_name
        response = self.http_request('put', url, json=json_data)
        data = parse_response(response)
        return data

    def modify_column_type(self, table_name, column_key, new_column_type):
        """
        :param table_name: str
        :param column_key: str
        :param new_column_type: str
        :return: dict
        """
        if new_column_type not in ColumnTypes:
            raise ValueError("type %s invalid!" % (new_column_type,))
        url = self._column_server_url()
        json_data = {
            'op_type': MODIFY_COLUMN_TYPE,
            'table_name': table_name,
            'column': column_key,
            'new_column_type': new_column_type.value
        }
        if like_table_id(table_name):
            json_data['table_id'] = table_name
        response = self.http_request('put', url, json=json_data)
        data = parse_response(response)
        return data

    def add_column_options(self, table_name, column, options):
        """
        :param table_name: str
        :param column: str
        :param options: list
        """
        url = self._column_options_server_url()
        json_data = {
            "table_name": table_name,
            "column": column,
            "options": options
        }
        if like_table_id(table_name):
            json_data['table_id'] = table_name
        response = self.http_request('post', url, json=json_data)
        data = parse_response(response)
        return data

    def add_column_cascade_settings(self, table_name, child_column, parent_column, cascade_settings):
        """

        :param table_name:  str
        :param child_column: str
        :param parent_column: str
        :param cascade_settings: dict
        :return:
        """
        url = self._column_cascade_setting_server_url()
        json_data = {
            "table_name": table_name,
            "child_column": child_column,
            "parent_column": parent_column,
            "cascade_settings": cascade_settings
        }
        if like_table_id(table_name):
            json_data['table_id'] = table_name
        response = self.http_request('post', url, json=json_data)
        data = parse_response(response)
        return data

    def delete_column(self, table_name, column_key):
        """
        :param table_name: str
        :param column_key: str
        :return: None
        """
        url = self._column_server_url()
        json_data = {
            'table_name': table_name,
            'column': column_key
        }
        if like_table_id(table_name):
            json_data['table_id'] = table_name
        response = self.http_request('delete', url, json=json_data)
        data = parse_response(response)
        return data

    def download_file(self, url, save_path):
        if not str(UUID(self.dtable_uuid)) in url:
            raise Exception('url invalid.')
        path = url.split(str(UUID(self.dtable_uuid)))[-1].strip('/')
        download_link = self.get_file_download_link(parse.unquote(path))
        response = self.http_request('get', download_link)
        if response.status_code != 200:
            raise Exception('download file error')
        with open(save_path, 'wb') as f:
            f.write(response.content)

    def upload_bytes_file(self, name, content: bytes, relative_path=None, file_type=None, replace=False):
        """
        relative_path: relative path for upload, if None, default {file_type}s/{date of this month} eg: files/2020-09
        file_type: if relative is None, file type must in ['image', 'file'], default 'file'
        return: info dict of uploaded file
        """
        upload_link_dict = self.get_file_upload_link()
        parent_dir = upload_link_dict['parent_path']
        upload_link = upload_link_dict['upload_link'] + '?ret-json=1'
        if not relative_path:
            if file_type and file_type not in ['image', 'file']:
                raise Exception('relative or file_type invalid.')
            if not file_type:
                file_type = 'file'
            relative_path = '%ss/%s' % (file_type, str(datetime.today())[:7])
        else:
            relative_path = relative_path.strip('/')
        response = self.http_request('post', upload_link, data={
            'parent_dir': parent_dir,
            'relative_path': relative_path,
            'replace': 1 if replace else 0
        }, files={
            'file': (name, io.BytesIO(content))
        })
        d = response.json()[0]
        url = '%(server)s/workspace/%(workspace_id)s/asset/%(dtable_uuid)s/%(relative_path)s/%(filename)s' % {
            'server': self.server_url.strip('/'),
            'workspace_id': self.workspace_id,
            'dtable_uuid': str(UUID(self.dtable_uuid)),
            'file_type': file_type,
            'relative_path': parse.quote(relative_path.strip('/')),
            'filename': parse.quote(d.get('name', name))
        }
        return {
            'type': file_type,
            'size': d.get('size'),
            'name': d.get('name'),
            'url': url
        }

    def upload_local_file(self, file_path, name=None, relative_path=None, file_type=None, replace=False):
        """
        relative_path: relative path for upload, if None, default {file_type}s/{date of today}, eg: files/2020-09
        file_type: if relative is None, file type must in ['image', 'file'], default 'file'
        return: info dict of uploaded file
        """
        if file_type not in ['image', 'file']:
            raise Exception('file_type invalid.')
        if not name:
            name = file_path.strip('/').split('/')[-1]
        if not relative_path:
            if file_type and file_type not in ['image', 'file']:
                raise Exception('relative or file_type invalid.')
            if not file_type:
                file_type = 'file'
            relative_path = '%ss/%s' % (file_type, str(datetime.today())[:7])
        else:
            relative_path = relative_path.strip('/')
        upload_link_dict = self.get_file_upload_link()
        parent_dir = upload_link_dict['parent_path']
        upload_link = upload_link_dict['upload_link'] + '?ret-json=1'
        response = self.http_request('post', upload_link, data={
            'parent_dir': parent_dir,
            'relative_path': relative_path,
            'replace': 1 if replace else 0
        }, files={
            'file': (name, open(file_path, 'rb'))
        })
        d = response.json()[0]
        url = '%(server)s/workspace/%(workspace_id)s/asset/%(dtable_uuid)s/%(relative_path)s/%(filename)s' % {
            'server': self.server_url.strip('/'),
            'workspace_id': self.workspace_id,
            'dtable_uuid': str(UUID(self.dtable_uuid)),
            'file_type': file_type,
            'relative_path': parse.quote(relative_path.strip('/')),
            'filename': parse.quote(d.get('name', name))
        }
        return {
            'type': file_type,
            'size': d.get('size'),
            'name': d.get('name'),
            'url': url
        }

    def filter(self, table_name, conditions='', view_name=None):
        """
        :param table_name: str
        :param conditions: str
        :return: queryset
        """
        base = self._clone()
        queryset = QuerySet(base, table_name)
        queryset.raw_rows = self.list_rows(table_name, view_name)
        queryset.raw_columns = self.list_columns(table_name, view_name)
        queryset.conditions = conditions
        queryset._execute_conditions()
        return queryset

    def query(self, sql, convert=True):
        """
        :param sql: str
        :param convert: bool
        :return: list
        """
        if not sql:
            raise ValueError('sql can not be empty.')
        url = self._dtable_db_query_url()
        json_data = {'sql': sql}
        response = self.http_request('post', url, json=json_data)
        data = parse_response(response)
        if not data.get('success'):
            raise Exception(data.get('error_message'))
        metadata = data.get('metadata')
        results = data.get('results')
        if convert:
            converted_results = convert_db_rows(metadata, results)
            return converted_results
        else:
            return results

    def get_related_users(self):
        response = self.http_request('get', self._get_related_users_url())
        return parse_response(response)['user_list']

    def send_toast_notification(self, user_id, msg, toast_type='success'):
        url = self._send_toast_notification_url()
        self.http_request('post', url, json={
            'to_user': user_id,
            'toast_type': toast_type,
            'detail': {
                'msg': str(msg)
            }
        })

    def add_workflow_task(self, workflow_token, row_data, initiator=None, link_rows=None, new_linked_rows=None):
        url = self._add_workflow_task_url(workflow_token)
        response = self.http_request('post', url, data={
            'row_data': json.dumps(row_data),
            'initiator': initiator,
            'link_rows': json.dumps(link_rows or []),
            'new_linked_rows': json.dumps(new_linked_rows or [])
        })
        return parse_response(response)['task']

    def add_workflow_task_with_existed_row(self, workflow_token, row_id, initiator=None):
        url = self._add_workflow_task_url(workflow_token)
        response = self.http_request('post', url, data={'row_id': row_id, 'initiator': initiator})
        return parse_response(response)['task']

    def big_data_insert_rows(self, table_name, rows_data):
        url = self._dtable_db_insert_rows_url()
        json_data = {
            'table_name': table_name,
            'rows': rows_data,
        }
        response = self.http_request('post', url, json=json_data)
        return parse_response(response)


class Account(ApiBase):
    def __init__(self, login_name, password, server_url, session=None, ssl_verify=True, timeout=30):
        self.login_name = login_name
        self.username = None
        self.password = password
        self.server_url = server_url.strip().strip('/')
        self.token = None
        super().__init__(session, timeout, ssl_verify)

    def __str__(self):
        return '<SeaTable Account [ %s ]>' % (self.login_name)

    def _get_api_token_url(self):
        return '%s/api2/auth-token/' % (self.server_url,)

    def _list_workspaces_url(self):
        return '%s/api/v2.1/workspaces/' % (self.server_url,)

    def _add_base_url(self):
        return '%s/api/v2.1/dtables/' % (self.server_url,)

    def _get_account_info_url(self):
        return '%s/api2/account/info/' % (self.server_url,)

    def _get_copy_dtable_url(self):
        return '%s/api/v2.1/dtable-copy/' % (self.server_url,)

    def _get_temp_api_token_url(self, workspace_id, name):
        return '%(server_url)s/api/v2.1/workspace/%(workspace_id)s/dtable/%(name)s/temp-api-token/' % {
            'server_url': self.server_url,
            'workspace_id': workspace_id,
            'name': name
        }


    def auth(self):
        response = self.http_request('post', self._get_api_token_url(), data={
            'username': self.login_name,
            'password': self.password
        })
        data = parse_response(response)
        self.token = data.get('token')

    def load_account_info(self):
        response = self.http_request('get', self._get_account_info_url(), headers=self.token_headers())
        self.username = parse_response(response).get('email')

    def list_workspaces(self):
        response = self.http_request('get', self._list_workspaces_url(), headers=self.token_headers())
        return parse_response(response)

    def add_base(self, name, workspace_id=None):
        owner = None
        if not workspace_id:
            if not self.username:
                self.load_account_info()  # load username for owner
            owner = self.username
        else:
            workspace_list = self.list_workspaces()['workspace_list']
            for w in workspace_list:
                if w.get('id') == workspace_id and w.get('group_id'):
                    owner = '%s@seafile_group' % w['group_id']
                    break
                if w.get('id') == workspace_id and w.get('owner_type') == 'Personal':
                    if not self.username:
                        self.load_account_info()  # load username for owner
                    owner = self.username
                    break
        if not owner:
            raise Exception('workspace_id invalid.')
        response = self.http_request('post', self._add_base_url(), data={
            'name': name,
            'owner': owner
        }, headers=self.token_headers())
        return parse_response(response).get('table')

    def copy_base(self, src_workspace_id, base_name, dst_workspace_id):
        response = self.http_request('post', self._get_copy_dtable_url(), data={
            'src_workspace_id': src_workspace_id,
            'name': base_name,
            'dst_workspace_id': dst_workspace_id
        }, headers=self.token_headers())
        return parse_response(response).get('dtable')

    def get_base(self, workspace_id, base_name, with_socket_io=False):
        response = self.http_request('get', self._get_temp_api_token_url(workspace_id, base_name),
            headers=self.token_headers())
        api_token = parse_response(response).get('api_token')
        base = SeaTableAPI(
            api_token,
            self.server_url,
            session=self.session,
            ssl_verify=self.ssl_verify,
            timeout=self.timeout
        )
        base.auth(with_socket_io=with_socket_io)
        return base
