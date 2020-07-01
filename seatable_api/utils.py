import json


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


def convert_row(metadata, data):
    # parse data
    data = json.loads(data)
    table_id = data['table_id']
    row_id = data['row_id']
    op_type = data['op_type']

    if op_type == 'modify_row':
        row = data['updated']
    elif op_type == 'insert_row':
        row = data['row_data']
    else:
        row = {}

    table = _get_table(metadata, table_id)
    columns = table['columns']
    column_map = _get_column_map(columns)

    # convert row
    result = {}
    result['_id'] = row_id
    result['op_type'] = op_type

    for column_key, cell_value in row.items():
        column = column_map.get(column_key)
        if not column:
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
