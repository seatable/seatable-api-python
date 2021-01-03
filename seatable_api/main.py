import io
import json
from datetime import datetime
from urllib import parse
from uuid import UUID

import requests

from .constants import ROW_FILTER_KEYS, ColumnTypes
from .constants import RENAME_COLUMN, RESIZE_COLUMN, FREEZE_COLUMN, MOVE_COLUMN, MODIFY_COLUMN_TYPE, DELETE_COLUMN
from .socket_io import connect_socket_io
from .query import QuerySet


def parse_headers(token):
    return {
        'Authorization': 'Token ' + token,
        'Content-Type': 'application/json',
    }


def parse_server_url(server_url):
    return server_url.rstrip('/')


def parse_response(response):
    if response.status_code >= 400:
        raise ConnectionError(response.status_code, response.text)
    else:
        try:
            data = json.loads(response.text)
            return data
        except:
            pass


class SeaTableAPI(object):
    """SeaTable API
    """

    def __init__(self, token, server_url):
        """
        :param token: str
        :param server_url: str
        """
        self.token = token
        self.server_url = parse_server_url(server_url)
        self.dtable_server_url = None
        self.dtable_uuid = None
        self.headers = None
        self.socketIO = None
        self.workspace_id = None
        self.dtable_name = None

    def __str__(self):
        return 'SeaTable Base [ %s ]' % self.dtable_uuid

    def _clone(self):
        clone = self.__class__(self.token, self.server_url)
        return clone

    def auth(self, with_socket_io=False):
        """Auth to SeaTable
        """
        url = self.server_url + '/api/v2.1/dtable/app-access-token/'
        headers = parse_headers(self.token)
        response = requests.get(url, headers=headers)
        data = parse_response(response)

        self.dtable_uuid = data.get('dtable_uuid')
        jwt_token = data.get('access_token')
        self.headers = parse_headers(jwt_token)
        self.dtable_server_url = parse_server_url(data.get('dtable_server'))
        self.workspace_id = data.get('workspace_id')
        self.dtable_name = data.get('dtable_name')

        if with_socket_io is True:
            self.socketIO = connect_socket_io(
                self.dtable_server_url, self.dtable_uuid, jwt_token)

    def _metadata_server_url(self):
        return self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/metadata/'

    def _row_server_url(self):
        return self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/rows/'

    def _batch_row_server_url(self):
        return self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/batch-append-rows/'

    def _batch_delete_row_server_url(self):
        return self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/batch-delete-rows/'

    def _filtered_rows_server_url(self):
        return self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/filtered-rows/'

    def _row_link_server_url(self):
        return self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/links/'

    def _app_download_link_url(self):
        return self.server_url + '/api/v2.1/dtable/app-download-link/'

    def _app_upload_link_url(self):
        return self.server_url + '/api/v2.1/dtable/app-upload-link/'

    def _column_server_url(self):
        return self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/columns/'

    def get_metadata(self):
        """
        :return: dict
        """
        url = self._metadata_server_url()
        response = requests.get(url, headers=self.headers)
        data = parse_response(response)
        return data.get('metadata')

    def list_rows(self, table_name, view_name=None):
        """
        :param table_name: str
        :param view_name: str
        :return: list
        """
        url = self._row_server_url()
        params = {
            'table_name': table_name,
        }
        if view_name:
            params['view_name'] = view_name
        response = requests.get(url, params=params, headers=self.headers)
        data = parse_response(response)
        return data.get('rows')

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
        response = requests.post(url, json=json_data, headers=self.headers)
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
        response = requests.post(url, json=json_data, headers=self.headers)
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
        response = requests.post(url, json=json_data, headers=self.headers)
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
        response = requests.put(url, json=json_data, headers=self.headers)
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
        response = requests.delete(url, json=json_data, headers=self.headers)
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
        response = requests.delete(url, json=json_data, headers=self.headers)
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
        response = requests.get(
            url, json=json_data, params=params, headers=self.headers)
        data = parse_response(response)
        return data.get('rows')

    def get_file_download_link(self, path):
        """
        :param path: str
        :return: str
        """
        url = self._app_download_link_url()
        params = {'path': path}
        headers = parse_headers(self.token)
        response = requests.get(url, params=params, headers=headers)
        data = parse_response(response)
        return data.get('download_link')

    def get_file_upload_link(self):
        """
        :return: dict
        """
        url = self._app_upload_link_url()
        headers = parse_headers(self.token)
        response = requests.get(url, headers=headers)
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
        response = requests.post(url, json=json_data, headers=self.headers)
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
        response = requests.delete(url, json=json_data, headers=self.headers)
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
        if view_name:
            params['view_name'] = view_name
        response = requests.get(url, params=params, headers=self.headers)
        data = parse_response(response)
        return data.get('columns')

    def insert_column(self, table_name, column_name, column_type, column_key=None):
        """
        :param table_name: str
        :param column_name: str
        :param column_type: ColumnType enum
        :param column_key: str, which you want to insert after
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
        if column_key:
            json_data['column_key'] = column_key
        response = requests.post(url, json=json_data, headers=self.headers)
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
            'column_key': column_key,
            'new_column_name': new_column_name
        }
        response = requests.put(url, json=json_data, headers=self.headers)
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
            'column_key': column_key,
            'new_column_width': new_column_width
        }
        response = requests.put(url, json=json_data, headers=self.headers)
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
            'column_key': column_key,
            'frozen': frozen
        }
        response = requests.put(url, json=json_data, headers=self.headers)
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
            'column_key': column_key,
            'target_column_key': target_column_key
        }
        response = requests.put(url, json=json_data, headers=self.headers)
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
            'column_key': column_key,
            'new_column_type': new_column_type.value
        }
        response = requests.put(url, json=json_data, headers=self.headers)
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
            'column_key': column_key
        }
        response = requests.delete(url, json=json_data, headers=self.headers)
        data = parse_response(response)
        return data

    def download_file(self, url, save_path):
        if not str(UUID(self.dtable_uuid)) in url:
            raise Exception('url invalid.')
        path = url.split(str(UUID(self.dtable_uuid)))[-1].strip('/')
        download_link = self.get_file_download_link(parse.unquote(path))
        response = requests.get(download_link)
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
        response = requests.post(upload_link, data={
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
        response = requests.post(upload_link, data={
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
        base.auth()
        queryset = QuerySet(base, table_name)
        queryset.raw_rows = self.list_rows(table_name, view_name)
        queryset.raw_columns = self.list_columns(table_name, view_name)
        queryset.conditions = conditions
        queryset._execute_conditions()
        return queryset


class Account(object):
    def __init__(self, login_name, password, server_url):
        self.login_name = login_name
        self.username = None
        self.password = password
        self.server_url = server_url.strip().strip('/')
        self.token = None

    def __str__(self):
        return 'SeaTable Account [ %s ]' % (self.login_name)

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

    @property
    def token_headers(self):
        return {
            'Authorization': 'Token %s' % (self.token,)
        }

    def auth(self):
        response = requests.post(self._get_api_token_url(), data={
            'username': self.login_name,
            'password': self.password
        })
        data = parse_response(response)
        self.token = data.get('token')

    def load_account_info(self):
        response = requests.get(self._get_account_info_url(), headers=self.token_headers)
        self.username = parse_response(response).get('email')

    def list_workspaces(self):
        response = requests.get(self._list_workspaces_url(), headers=self.token_headers)
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
        response = requests.post(self._add_base_url(), data={
            'name': name,
            'owner': owner
        }, headers=self.token_headers)
        return parse_response(response).get('table')

    def copy_base(self, src_workspace_id, base_name, dst_workspace_id):
        response = requests.post(self._get_copy_dtable_url(), data={
            'src_workspace_id': src_workspace_id,
            'name': base_name,
            'dst_workspace_id': dst_workspace_id
        }, headers=self.token_headers)
        return parse_response(response).get('dtable')

    def get_base(self, workspace_id, base_name, with_socket_io=False):
        response = requests.get(self._get_temp_api_token_url(workspace_id, base_name), headers=self.token_headers)
        api_token = parse_response(response).get('api_token')
        base = SeaTableAPI(api_token, self.server_url)
        base.auth(with_socket_io=with_socket_io)
        return base
