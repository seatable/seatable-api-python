import json
import re
from datetime import datetime

from seatable_api.exception import AuthExpiredError


def _get_row(data):
    op_type = data['op_type']
    if op_type == 'insert_row':
        row = data['row_data']
    elif op_type == 'modify_row':
        row = data['updated']
    elif op_type == 'delete_row':
        row = data['deleted_row']
    else:
        row = None

    return row


def _get_table(metadata, table_id):
    tables = metadata['tables']
    table_filter = [table for table in tables if table['_id'] == table_id]
    table = table_filter[0]

    return table


def _get_column_map(columns):
    column_map = {}
    for column in columns:
        column_key = column['key']
        column_map[column_key] = column

    return column_map


def _get_option_name(options, option_id):
    option_filter = [option for option in options if option['id'] == option_id]
    option = option_filter[0]
    option_name = option['name']

    return option_name


def convert_row(metadata, ws_data):
    """ Convert websocket row data to readable row data

    :param metadata: dict
    :param ws_data: str
    :return: dict
    """
    data = json.loads(ws_data)
    row = _get_row(data)
    if not row:
        return data

    table = _get_table(metadata, data['table_id'])
    column_map = _get_column_map(table['columns'])

    # convert row
    result = {}
    result['_id'] = data['row_id']
    result['op_type'] = data['op_type']
    result['table_name'] = table['name']

    for column_key, cell_value in row.items():
        column = column_map.get(column_key)
        if not column:
            # inner data
            continue

        column_name = column['name']
        column_type = column['type']

        if column_type == 'single-select':
            if cell_value:
                options = column.get('data', {}).get('options')
                if not options:
                    continue
                result[column_name] = _get_option_name(options, cell_value)
            else:
                result[column_name] = cell_value

        elif column_type == 'multiple-select':
            if cell_value:
                options = column.get('data', {}).get('options')
                if not options:
                    continue
                result[column_name] = [_get_option_name(
                    options, option_id) for option_id in cell_value]
            else:
                result[column_name] = cell_value

        elif column_type == 'long-text':
            result[column_name] = cell_value['text'] if cell_value else ''

        else:
            result[column_name] = cell_value

    return result


def is_single_multiple_structure(column):
    column_type = column['type']
    if column_type in ('single-select', 'multiple-select'):
        column_data = column.get('data', {})
        options = column_data and column_data.get('options', []) or []
        return True, options
    if column_type in ('link', 'link-formula'):
        array_type = column.get('data', {}).get('array_type')
        if array_type in ('single-select', 'multiple-select'):
            array_data = column.get('data', {}).get('array_data', {})
            options = array_data and array_data.get('options', []) or []
            return True, options
    return False, []

def convert_db_rows(metadata, results):
    """ Convert dtable-db rows data to readable rows data

    :param metadata: list
    :param results: list
    :return: list
    """
    if not results:
        return []
    converted_results = []
    column_map = {column['key']: column for column in metadata}
    select_map = {}
    for column in metadata:
            is_sm_structure, column_options = is_single_multiple_structure(column)
            if is_sm_structure:
                column_data = column['data']
                if not column_data:
                    continue
                column_key = column['key']
                select_map[column_key] = {
                    select['id']: select['name'] for select in column_options}

    for result in results:
        item = {}
        for column_key, value in result.items():
            if column_key in column_map:
                column = column_map[column_key]
                column_name = column['name']
                column_type = column['type']
                s_map = select_map.get(column_key)
                if column_type == 'single-select' and value and s_map:
                    item[column_name] = s_map.get(value, value)
                elif column_type == 'multiple-select' and value and s_map:
                    item[column_name] = [s_map.get(s, s) for s in value]
                elif column_type == 'link' and value and s_map:
                    new_data = []
                    for s in value:
                        old_display_value = s.get('display_value')
                        if isinstance(old_display_value, list):
                            s['display_value'] = old_display_value and [s_map.get(v, v) for v in old_display_value] or []
                        else:
                            s['display_value'] = s_map.get(old_display_value, old_display_value)
                        new_data.append(s)
                    item[column_name] = new_data
                elif column_type == 'link-formula' and value and s_map:
                    if isinstance(value[0], list):
                        item[column_name] = [[s_map.get(v, v) for v in s] for s in value]
                    else:
                        item[column_name] = [s_map.get(s, s) for s in value]

                elif column_type == 'date':
                    try:
                        if value:
                            date_value = datetime.fromisoformat(value)
                            date_format = column['data']['format']
                            if date_format == 'YYYY-MM-DD':
                                value = date_value.strftime('%Y-%m-%d')
                            else:
                                value = date_value.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            value = None
                    except Exception as e:
                        print('[Warning] format date:', e)
                    item[column_name] = value
                else:
                    item[column_name] = value
            else:
                item[column_key] = value
        converted_results.append(item)

    return converted_results


def parse_headers(token):
    return {
        'Authorization': 'Token ' + token,
        'Content-Type': 'application/json',
    }


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
