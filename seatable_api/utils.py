import json
from datetime import datetime


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
            column_type = column['type']
            if column_type in ('single-select', 'multiple-select'):
                column_data = column['data']
                if not column_data:
                    continue
                column_key = column['key']
                column_options = column['data']['options']
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
