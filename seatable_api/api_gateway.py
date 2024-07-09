import requests

from .constants import ROW_FILTER_KEYS, ColumnTypes
from .constants import RENAME_COLUMN, RESIZE_COLUMN, FREEZE_COLUMN, MOVE_COLUMN, MODIFY_COLUMN_TYPE, DELETE_COLUMN
from .utils import convert_db_rows, parse_response, like_table_id, parse_headers


class APIGateway(object):
    """SeaTable API
    """

    def __init__(
            self,
            token,
            api_gateway_url,
            server_url,
            headers,
            dtable_uuid
    ):


        self.api_gateway_url = api_gateway_url
        self.server_url = server_url
        self.headers = headers
        self.dtable_uuid = dtable_uuid
        self._cache_table_name_id_map = {}
        self.token = token
        self.timeout = 30

    def _metadata_server_url(self):
        return self.api_gateway_url + '/api/v2/dtables/' + self.dtable_uuid + '/metadata/'

    def _table_server_url(self):
        return self.api_gateway_url + '/api/v2/dtables/' + self.dtable_uuid + '/tables/'

    def _view_server_url(self):
        return self.api_gateway_url + '/api/v2/dtables/' + self.dtable_uuid + '/views/'

    def _row_server_url(self):
        return self.api_gateway_url + '/api/v2/dtables/' + self.dtable_uuid + '/rows/'

    def _batch_row_server_url(self):
        return self._row_server_url()

    def _batch_update_row_server_url(self):
        return self._row_server_url()

    def _batch_delete_row_server_url(self):
        return self._row_server_url()

    def _filtered_rows_server_url(self):
        return self.api_gateway_url + '/api/v2/dtables/' + self.dtable_uuid + '/filtered-rows/'

    def _row_link_server_url(self):
        return self.api_gateway_url + '/api/v2/dtables/' + self.dtable_uuid + '/links/'

    def _query_links_server_url(self):
        return self.api_gateway_url + '/api/v2/dtables/' + self.dtable_uuid + '/query-links/'

    def _batch_update_row_link_server_url(self):
        return self._row_link_server_url()

    def _column_server_url(self):
        return self.api_gateway_url + '/api/v2/dtables/' + self.dtable_uuid + '/columns/'

    def _column_options_server_url(self):
        return self.api_gateway_url + '/api/v2/dtables/' + self.dtable_uuid + '/column-options/'

    def _column_cascade_setting_server_url(self):
        return self.api_gateway_url + '/api/v2/dtables/' + self.dtable_uuid + '/column-cascade-settings/'

    def _dtable_db_query_url(self):
        return self.api_gateway_url + '/api/v2/dtables/' + self.dtable_uuid + '/sql'

    def _dtable_db_insert_rows_url(self):
        return self.api_gateway_url + '/api/v2/dtables/' + self.dtable_uuid + '/add-archived-rows/'

    def _send_toast_notification_url(self):
        return '%(dtable_server_url)s/api/v2/dtables/%(dtable_uuid)s/ui-toasts/' % {
            'dtable_server_url': self.api_gateway_url,
            'dtable_uuid': self.dtable_uuid
        }

    def get_metadata(self):
        """
        :return: dict
        """
        url = self._metadata_server_url()
        response = requests.get(url, headers=self.headers, timeout=self.timeout)
        data = parse_response(response)
        return data.get('metadata')

    def list_tables(self):
        meta = self.get_metadata()
        return meta.get('tables') or []

    def get_table_by_name(self, table_name):
        tables = self.list_tables()
        for t in tables:
            if t.get('name') == table_name:
                return t

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
        response = requests.post(url, json=json_data, headers=self.headers, timeout=self.timeout)
        return parse_response(response)

    def rename_table(self, table_name, new_table_name):
        url = self._table_server_url()
        json_data = {
            'table_name': table_name,
            'new_table_name': new_table_name
        }
        response = requests.put(url, json=json_data, headers=self.headers, timeout=self.timeout)
        return parse_response(response)

    def delete_table(self, table_name):
        url = self._table_server_url()
        json_data = {
            'table_name': table_name,
        }
        response = requests.delete(url, json=json_data, headers=self.headers, timeout=self.timeout)
        return parse_response(response)

    def list_views(self, table_name):
        url = self._view_server_url()
        params = {
            'table_name': table_name
        }
        response = requests.get(url, params=params, headers=self.headers, timeout=self.timeout)
        return parse_response(response)

    def get_view_by_name(self, table_name, view_name):
        url = self._view_server_url()
        view_url = '%(url)s/%(view_name)s/?table_name=%(table_name)s' % ({
            "url": url.rstrip('/'),
            'view_name': view_name,
            'table_name': table_name
        })
        response = requests.get(view_url, headers=self.headers, timeout=self.timeout)
        return parse_response(response)


    def add_view(self, table_name, view_name):
        url = self._view_server_url()
        view_url = '%(url)s/?table_name=%(table_name)s' % ({
            "url": url.rstrip('/'),
            'table_name': table_name
        })
        json_data = {
            'name': view_name
        }
        response = requests.post(view_url, json=json_data, headers=self.headers, timeout=self.timeout)
        return parse_response(response)


    def rename_view(self, table_name, view_name, new_view_name):
        url = self._view_server_url()
        view_url = '%(url)s/%(view_name)s/?table_name=%(table_name)s' % ({
            "url": url.rstrip('/'),
            'view_name': view_name,
            'table_name': table_name
        })
        json_data = {
            'name': new_view_name
        }
        response = requests.put(view_url, json=json_data, headers=self.headers, timeout=self.timeout)
        return parse_response(response)


    def delete_view(self, table_name, view_name):
        url = self._view_server_url()
        view_url = '%(url)s/%(view_name)s/?table_name=%(table_name)s' % ({
            "url": url.rstrip('/'),
            'view_name': view_name,
            'table_name': table_name
        })
        response = requests.delete(view_url, headers=self.headers, timeout=self.timeout)
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
            'table_name': table_name,
            'convert_keys': True
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
        response = requests.get(url, params=params, headers=self.headers, timeout=self.timeout)
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
            'table_name': table_name,
            'convert_keys': True
        }

        if like_table_id(table_name):
            params['table_id'] = table_name
        response = requests.get(url, params=params, headers=self.headers, timeout=self.timeout)
        data = parse_response(response)
        return data


    def append_row(self, table_name, row_data, apply_default=None):
        """
        :param table_name: str
        :param row_data: dict
        """
        url = self._row_server_url()
        json_data = {
            'table_name': table_name,
            'rows': [row_data,]
        }
        if like_table_id(table_name):
            json_data['table_id'] = table_name
        if apply_default is not None:
            json_data['apply_default'] = apply_default
        response = requests.post(url, json=json_data, headers=self.headers, timeout=self.timeout)
        data = parse_response(response)
        return data.get('first_row')


    def batch_append_rows(self, table_name, rows_data, apply_default=None):
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
        if apply_default is not None:
            json_data['apply_default'] = apply_default
        response = requests.post(url, json=json_data, headers=self.headers, timeout=self.timeout)
        return parse_response(response)


    def insert_row(self, table_name, row_data, anchor_row_id, apply_default=None):
        """
        :param table_name: str
        :param row_data: dict
        :param anchor_row_id: str
        """
        return self.append_row(table_name, row_data, apply_default=apply_default)


    def update_row(self, table_name, row_id, row_data):
        """
        :param table_name: str
        :param row_id: str
        :param row_data: dict
        """
        url = self._row_server_url()
        json_data = {
            'table_name': table_name,
            'updates': [{
                'row_id': row_id,
                'row': row_data
            }]
        }
        response = requests.put(url, json=json_data, headers=self.headers, timeout=self.timeout)
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
        response = requests.put(url, json=json_data, headers=self.headers, timeout=self.timeout)
        return parse_response(response)


    def delete_row(self, table_name, row_id):
        """
        :param table_name: str
        :param row_id: str
        """
        url = self._row_server_url()
        json_data = {
            'table_name': table_name,
            'row_ids' : [row_id, ]
        }
        if like_table_id(table_name):
            json_data['table_id'] = table_name
        response = requests.delete(url, json=json_data, headers=self.headers, timeout=self.timeout)
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
        response = requests.delete(url, json=json_data, headers=self.headers, timeout=self.timeout)
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
            url, json=json_data, params=params, headers=self.headers, timeout=self.timeout)
        data = parse_response(response)
        return data.get('rows')


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
            'other_rows_ids_map': {row_id: [other_row_id, ]}
        }
        if like_table_id(table_name):
            json_data['table_id'] = table_name
        if like_table_id(other_table_name):
            json_data['other_table_id'] = other_table_name

        response = requests.post(url, json=json_data, headers=self.headers, timeout=self.timeout)
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
            'other_rows_ids_map': {row_id: [other_row_id, ]}
        }
        if like_table_id(table_name):
            json_data['table_id'] = table_name
        if like_table_id(other_table_name):
            json_data['other_table_id'] = other_table_name
        response = requests.delete(url, json=json_data, headers=self.headers, timeout=self.timeout)
        return parse_response(response)


    def update_link(self, link_id, table_name, other_table_name, row_id, other_rows_ids):
        """
        :param link_id: str
        :param table_name: str
        :param other_table_name: str
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
            'row_id_list': [row_id, ],
            'other_rows_ids_map': {row_id: other_rows_ids}
        }
        if like_table_id(table_name):
            json_data['table_id'] = table_name
        if like_table_id(other_table_name):
            json_data['other_table_id'] = other_table_name

        response = requests.put(url, json=json_data, headers=self.headers, timeout=self.timeout)
        return parse_response(response)


    def batch_update_links(self, link_id, table_name, other_table_name, row_id_list, other_rows_ids_map):
        """
        :param link_id: str
        :param table_name: str
        :param other_table_name: str
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
        response = requests.put(url, json=json_data, headers=self.headers, timeout=self.timeout)
        return parse_response(response)


    def get_linked_records(self, table_id, link_column_key, rows):
        """
        :param table_id:  str
        :param link_column_key: str
        :param rows: list
        """
        url = self._query_links_server_url()
        json_data = {
            'table_id': table_id,
            'link_column_key': link_column_key,
            'rows': rows,
        }
        response = requests.post(url, json=json_data, headers=self.headers, timeout=self.timeout)
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
        response = requests.get(url, params=params, headers=self.headers, timeout=self.timeout)
        data = parse_response(response)
        return data.get('columns')


    def get_column_link_id(self, table_name, column_name):
        columns = self.list_columns(table_name)
        for column in columns:
            if column.get('name') == column_name and column.get('type') == 'link':
                return column.get('data', {}).get('link_id')
        raise ValueError('link type column "%s" does not exist in current table' % column_name)


    def get_column_by_name(self, table_name, column_name):
        columns = self.list_columns(table_name)
        for col in columns:
            if col.get('name') == column_name:
                return col


    def get_columns_by_type(self, table_name, column_type: ColumnTypes):
        if column_type not in ColumnTypes:
            raise ValueError("type %s invalid!" % (column_type,))
        columns = self.list_columns(table_name)
        cols_results = [col for col in columns if col.get('type') == column_type.value]
        return cols_results


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
        response = requests.post(url, json=json_data, headers=self.headers, timeout=self.timeout)
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
        response = requests.put(url, json=json_data, headers=self.headers, timeout=self.timeout)
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
        response = requests.put(url, json=json_data, headers=self.headers, timeout=self.timeout)
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
        response = requests.put(url, json=json_data, headers=self.headers, timeout=self.timeout)
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
        response = requests.put(url, json=json_data, headers=self.headers, timeout=self.timeout)
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
        response = requests.put(url, json=json_data, headers=self.headers, timeout=self.timeout)
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
        response = requests.post(url, json=json_data, headers=self.headers, timeout=self.timeout)
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
        response = requests.post(url, json=json_data, headers=self.headers, timeout=self.timeout)
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
        response = requests.delete(url, json=json_data, headers=self.headers, timeout=self.timeout)
        data = parse_response(response)
        return data


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
        response = requests.post(url, json=json_data, headers=self.headers, timeout=self.timeout)
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

    def send_toast_notification(self, username, msg, toast_type='success'):
        url = self._send_toast_notification_url()
        requests.post(url, json={
            'to_user': username,
            'toast_type': toast_type,
            'detail': {
                'msg': str(msg)
            }
        }, headers=self.headers)


    def big_data_insert_rows(self, table_name, rows_data):
        url = self._dtable_db_insert_rows_url()
        json_data = {
            'table_name': table_name,
            'rows': rows_data,
        }
        response = requests.post(url, json=json_data, headers=self.headers, timeout=self.timeout)
        return parse_response(response)
