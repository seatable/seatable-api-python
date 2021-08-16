import re
import time
import random
import requests
from datetime import datetime

from .constants import ColumnTypes


HREF_REG = r'\[.+\]\(\S+\)|<img src=\S+.+\/>|!\[\]\(\S+\)|<\S+>'
LINK_REG_1 = r'^\[.+\]\((\S+)\)'
LINK_REG_2 = r'^<(\S+)>$'
IMAGE_REG_1 = r'^<img src="(\S+)" .+\/>'
IMAGE_REG_2 = r'^!\[\]\((\S+)\)'
LIMIT = 1000
DATE_FORMAT = '%Y-%m-%d'
DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'
AIRTABLE_API_URL = 'https://api.airtable.com/v0/'


class RowsConvertor(object):

    def convert(self, columns, airtable_rows):
        rows = self.gen_rows(columns, airtable_rows)
        return rows

    def parse_date(self, value):
        value = str(value)
        if len(value) == 10:
            value = str(datetime.strptime(value, DATE_FORMAT))
        elif len(value) == 24:
            value = str(datetime.strptime(value, DATETIME_FORMAT))
        return value

    def parse_file(self, value):
        files = []
        for item in value:
            file = {
                'name': item.get('filename'),
                'size': item.get('size'),
                'type': 'file',
                'url': item.get('url'),
            }
            files.append(file)
        return files

    def parse_collaborator(self, value):
        collaborators = []
        if isinstance(value, dict):
            collaborators.append(value.get('email'))
        elif isinstance(value, list):
            for item in value:
                collaborators.append(item.get('email'))
        return collaborators

    def parse_long_text(self, value):
        value = str(value)
        checked_count = value.count('[x]')
        unchecked_count = value.count('[ ]')
        total = checked_count + unchecked_count

        href_reg = re.compile(HREF_REG)
        preview = href_reg.sub(' ', value)
        preview = preview[:20].replace('\n', ' ')

        images = []
        links = []
        href_list = href_reg.findall(value)
        for href in href_list:
            if re.search(LINK_REG_1, href):
                links.append(re.search(LINK_REG_1, href).group(1))
            elif re.search(LINK_REG_2, href):
                links.append(re.search(LINK_REG_2, href).group(1))
            elif re.search(IMAGE_REG_1, href):
                images.append(re.search(IMAGE_REG_1, href).group(1))
            elif re.search(IMAGE_REG_2, href):
                images.append(re.search(IMAGE_REG_2, href).group(1))

        return {
            'text': value,
            'preview': preview,
            'checklist': {'completed': checked_count, 'total': total},
            'images': images,
            'links': links,
        }

    def gen_cell_data(self, column_type, value):
        try:
            if column_type == ColumnTypes.CHECKBOX:
                cell_data = value
            elif column_type == (
                    ColumnTypes.NUMBER, ColumnTypes.DURATION, ColumnTypes.RATE):
                cell_data = value
            elif column_type in (
                    ColumnTypes.TEXT, ColumnTypes.SINGLE_SELECT, ColumnTypes.URL, ColumnTypes.EMAIL, ColumnTypes.MTIME, ColumnTypes.CTIME):
                cell_data = str(value)
            elif column_type in (ColumnTypes.MULTIPLE_SELECT, ColumnTypes.LINK):
                cell_data = value
            elif column_type == ColumnTypes.DATE:
                cell_data = self.parse_date(value)
            elif column_type == ColumnTypes.LONG_TEXT:
                cell_data = self.parse_long_text(value)
            elif column_type == ColumnTypes.COLLABORATOR:
                cell_data = self.parse_collaborator(value)
            elif column_type == ColumnTypes.FILE:
                cell_data = self.parse_file(value)
            elif column_type == ColumnTypes.BUTTON:
                cell_data = None
            else:
                cell_data = str(value)
        except Exception as e:
            print('[Error]', e)
            cell_data = str(value)
        return cell_data

    def gen_rows(self, columns, airtable_rows):
        rows = []
        for row in airtable_rows:
            row_data = {
                '_id': row.get('_id'),
            }
            for column in columns:
                column_name = column['name']
                column_type = ColumnTypes(column['type'])
                value = row.get(column_name)
                if value is None:
                    continue
                cell_data = self.gen_cell_data(column_type, value)
                row_data[column_name] = cell_data
            if row_data:
                rows.append(row_data)
        return rows


class ColumnsParser(object):

    def parse(self, airtable_rows):
        value_map = self.get_value_map(airtable_rows)
        columns = self.gen_columns(value_map)
        return columns

    def get_value_map(self, airtable_rows):
        value_map = {}
        for row in airtable_rows:
            for column_name, value in row.items():
                if column_name in value_map:
                    value_map[column_name].append(value)
                else:
                    value_map[column_name] = [value]
        return value_map

    def random_color(self):
        color_str = '0123456789ABCDEF'
        color = '#'
        for i in range(6):
            color += random.choice(color_str)
        return color

    def random_num_id(self):
        num_str = '0123456789'
        num = ''
        for i in range(6):
            num += random.choice(num_str)
        return num

    def parse_values(self, values):
        column_type = ColumnTypes.TEXT
        column_data = None
        try:
            type_list = []
            for value in values:
                if value is None:
                    continue

                elif value is True:
                    column_type = ColumnTypes.CHECKBOX

                elif isinstance(value, int) or isinstance(value, float):
                    column_type = ColumnTypes.NUMBER

                elif isinstance(value, str):
                    if '-' in value and len(value) == 10:
                        try:
                            datetime.strptime(value, DATE_FORMAT)
                            column_type = ColumnTypes.DATE
                        except ValueError:
                            pass
                    elif '-' in value and len(value) == 24:
                        try:
                            datetime.strptime(value, DATETIME_FORMAT)
                            column_type = ColumnTypes.DATE
                        except ValueError:
                            pass
                    elif value.startswith('http://') or value.startswith('https://'):
                        column_type = ColumnTypes.URL
                    elif '@' in value and '.' in value:
                        column_type = ColumnTypes.EMAIL
                    elif '\n' in value or len(value) > 50:
                        column_type = ColumnTypes.LONG_TEXT

                elif isinstance(value, list):
                    if not value:
                        continue
                    elif isinstance(value[0], str):
                        if value[0].startswith('rec'):
                            column_type = ColumnTypes.LINK
                            column_data = {}
                        else:
                            column_type = ColumnTypes.MULTIPLE_SELECT
                    elif isinstance(value[0], dict):
                        if 'email' in value[0]:
                            column_type = ColumnTypes.COLLABORATOR
                        elif 'filename' in value[0]:
                            column_type = ColumnTypes.FILE

                elif isinstance(value, dict):
                    if not value:
                        continue
                    elif 'email' in value:
                        column_type = ColumnTypes.COLLABORATOR
                    elif 'label' in value:
                        column_type = ColumnTypes.BUTTON
                    elif 'text' in value:
                        column_type = ColumnTypes.TEXT

                type_list.append(column_type)

            column_type = max(
                type_list, key=type_list.count) if type_list else ColumnTypes.TEXT

            if column_type == ColumnTypes.MULTIPLE_SELECT:
                multiple_list = []
                for value in values:
                    if not isinstance(value, list):
                        continue
                    for item in value:
                        if item not in multiple_list:
                            multiple_list.append(item)
                column_data = {
                    'options': [{'name': value, 'id': self.random_num_id(), 'color': self.random_color()} for value in multiple_list]}
        except Exception as e:
            print('[Error]', e)
        return column_type, column_data

    def gen_columns(self, value_map):
        columns = []
        for column_name, values in value_map.items():
            if column_name in ('_id', 'Name', '名称'):
                continue
            column_type, column_data = self.parse_values(values)
            if column_type == ColumnTypes.LINK:
                print(
                    '[Warning] You should add column [ %s ](LINK) manually' % column_name)
                continue
            column = {
                'name': column_name,
                'type': column_type,
                'data': column_data,
            }
            columns.append(column)
        return columns


class AirtableAPI(object):

    def __init__(self, airtable_api_key, airtable_base_id):
        self.airtable_api_key = airtable_api_key
        self.airtable_base_id = airtable_base_id

    def __str__(self):
        return '<Airtable API [ %s ]>' % (self.airtable_base_id)

    def list_rows(self, table_name, offset=''):
        headers = {'Authorization': 'Bearer ' + self.airtable_api_key}
        url = AIRTABLE_API_URL + self.airtable_base_id + '/' + table_name
        if offset:
            url = url + '?offset=' + offset
        response = requests.get(url, headers=headers, timeout=60)
        if response.status_code >= 400:
            raise ConnectionError(response.status_code, response.text)
        response_dict = response.json()
        offset = response_dict.get('offset')
        records = response_dict['records']
        rows = []
        for record in records:
            row = record.get('fields')
            row['_id'] = record.get('id')
            rows.append(row)
        return rows, offset

    def list_all_rows(self, table_name):
        all_rows = []
        offset = ''
        while True:
            rows, offset = self.list_rows(table_name, offset)
            all_rows.extend(rows)
            print(
                '[Info] Got [ %s ] rows in Airtable <%s>' % (len(rows), table_name))
            if not offset:
                break
            time.sleep(0.5)
        return all_rows


class AirtableConvertor(object):

    def __init__(self, airtable_api_key, airtable_base_id, base, table_names):
        self.airtable_api = AirtableAPI(airtable_api_key, airtable_base_id)
        self.columns_parser = ColumnsParser()
        self.rows_convertor = RowsConvertor()
        self.base = base
        self.table_names = table_names

    def auto_convert(self):
        self.convert_tables()
        self.convert_rows()

    def convert_tables(self):
        metadata = self.get_metadata()
        existing_tables = metadata.get('tables', [])
        for table_name in self.table_names:
            table_exists, existing_table = self.table_exists(
                existing_tables, table_name)
            existing_columns = existing_table.get('columns', [])
            if not table_exists:
                self.add_table(table_name)
                print('[Info] Added table [ %s ]' % table_name)
            airtable_rows, offset = self.airtable_api.list_rows(table_name)
            columns = self.columns_parser.parse(airtable_rows)
            for column in columns:
                column_exists, existing_column = self.column_exists(
                    existing_columns, column['name'])
                if not column_exists:
                    self.add_column(
                        table_name, column['name'], column['type'], column['data'])
                    print(
                        '[Info] Added column [ %s ] to table <%s>' % (column['name'], table_name))
            time.sleep(1)
        print('[Info] Convert tables success\n')

    def convert_rows(self):
        for table_name in self.table_names:
            columns = self.list_columns(table_name)
            airtable_rows = self.airtable_api.list_all_rows(table_name)
            rows = self.rows_convertor.convert(columns, airtable_rows)
            self.batch_append_rows(table_name, rows)
            time.sleep(1)
        print('[Info] Convert rows success\n')

    def get_metadata(self):
        metadata = self.base.get_metadata()
        return metadata

    def table_exists(self, existing_tables, table_name):
        for table in existing_tables:
            if table_name == table.get('name'):
                return True, table
        return False, {}

    def column_exists(self, existing_columns, column_name):
        for column in existing_columns:
            if column_name == column.get('name'):
                return True, column
        return False, {}

    def add_table(self, table_name):
        self.base.add_table(table_name)

    def add_column(self, table_name, column_name, column_type, column_data=None):
        self.base.insert_column(
            table_name, column_name, column_type, None, column_data)

    def list_columns(self, table_name):
        columns = self.base.list_columns(table_name)
        return columns

    def batch_append_rows(self, table_name, rows):
        offset = 0
        while True:
            row_split = rows[offset: LIMIT]
            offset = offset + LIMIT
            if not row_split:
                break
            self.base.batch_append_rows(table_name, row_split)
            print(
                '[Info] Appended [ %s ] rows to table <%s>' % (len(row_split), table_name))
            time.sleep(0.5)
