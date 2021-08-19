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
ColumnTypes.BARCODE = 'barcode'


class LinksConvertor(object):

    def convert(self, columns, airtable_rows):
        rows = self.gen_rows(columns, airtable_rows)
        return rows


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
        return [{
                'name': item['filename'],
                'size': item['size'],
                'type': 'file',
                'url': item['url'],
                } for item in value]

    def parse_multiple_select(self, value):
        if isinstance(value[0], str):
            return value
        # collaborators
        collaborators = []
        if isinstance(value[0], list):
            for item in value:
                collaborators.append(item['name'])
        return collaborators

    def parse_text(self, value):
        if isinstance(value, dict):
            if 'name' in value:  # collaborator
                return value['name']
            elif 'text' in value:  # barcode
                return value['text']
        value = str(value)
        return value

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
                    ColumnTypes.NUMBER, ColumnTypes.AUTO_NUMBER, ColumnTypes.DURATION, ColumnTypes.RATE):
                cell_data = value
            elif column_type in (
                    ColumnTypes.SINGLE_SELECT, ColumnTypes.URL, ColumnTypes.EMAIL):
                cell_data = value
            elif column_type == ColumnTypes.LINK:
                cell_data = None
            elif column_type == ColumnTypes.BUTTON:
                cell_data = None
            elif column_type == ColumnTypes.COLLABORATOR:
                cell_data = None
            elif column_type == ColumnTypes.TEXT:
                cell_data = self.parse_text(value)
            elif column_type == ColumnTypes.LONG_TEXT:
                cell_data = self.parse_long_text(value)
            elif column_type == ColumnTypes.DATE:
                cell_data = self.parse_date(value)
            elif column_type == ColumnTypes.FILE:
                cell_data = self.parse_file(value)
            elif column_type == ColumnTypes.MULTIPLE_SELECT:
                cell_data = self.parse_multiple_select(value)
            else:
                cell_data = str(value)
        except Exception as e:
            print('[Error] gen cell data error:', e)
            cell_data = str(value)
        return cell_data

    def gen_rows(self, columns, airtable_rows):
        rows = []
        for row in airtable_rows:
            row_data = {'_id': row['_id']}
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

    def parse(self, link_map, table_name, airtable_rows):
        value_map = self.get_value_map(airtable_rows)
        columns = self.gen_columns(link_map, table_name, value_map)
        return columns

    def get_value_map(self, airtable_rows):
        value_map = {}
        for row in airtable_rows:
            for column_name, value in row.items():
                if column_name not in value_map:
                    value_map[column_name] = []
                if value is not None:
                    value_map[column_name].append(value)
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

    def get_multiple_select_data(self, multiple_list):
        return {'options': [{
            'name': value,
            'id': self.random_num_id(),
            'color': self.random_color()
        } for value in multiple_list]}

    def get_column_data(self, link_map, table_name, column_name, column_type, values):
        column_data = None
        try:
            if column_type == ColumnTypes.BARCODE:
                column_type = ColumnTypes.TEXT
            # elif column_type == ColumnTypes.DATE and len(values[0]) == 24:
            #     column_data = {'format': 'YYYY-MM-DD HH:mm'}
            elif column_type == ColumnTypes.LINK:
                link = link_map[table_name][column_name]
                other_table_name = link[2]
                column_data = {'other_table': other_table_name}
            elif column_type == ColumnTypes.MULTIPLE_SELECT:
                multiple_list = []
                for value in values:
                    for item in value:
                        if item not in multiple_list:
                            multiple_list.append(item)
                column_data = self.get_multiple_select_data(multiple_list)
            elif column_type == ColumnTypes.COLLABORATOR:
                if isinstance(values[0], dict):
                    column_type = ColumnTypes.TEXT
                elif isinstance(values[0], list):
                    column_type = ColumnTypes.MULTIPLE_SELECT
                    multiple_list = []
                    for value in values:
                        for item in value:
                            name = item['name']
                            if name not in multiple_list:
                                multiple_list.append(name)
                    column_data = self.get_multiple_select_data(multiple_list)
        except Exception as e:
            print('[Error] get column data error:', e)
        return column_type, column_data

    def get_column_type(self, values):
        column_type = ColumnTypes.TEXT
        try:
            type_list = []
            for value in values:
                # boolean
                if value is True:
                    column_type = ColumnTypes.CHECKBOX
                # number
                elif isinstance(value, int) or isinstance(value, float):
                    column_type = ColumnTypes.NUMBER
                # str
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
                # list
                elif isinstance(value, list):
                    if not value:
                        continue
                    elif isinstance(value[0], str):
                        if value[0].startswith('rec'):
                            column_type = ColumnTypes.LINK
                        else:
                            column_type = ColumnTypes.MULTIPLE_SELECT
                    elif isinstance(value[0], dict):
                        if 'email' in value[0]:
                            column_type = ColumnTypes.COLLABORATOR
                        elif 'filename' in value[0]:
                            column_type = ColumnTypes.FILE
                # dict
                elif isinstance(value, dict):
                    if not value:
                        continue
                    elif 'email' in value:
                        column_type = ColumnTypes.COLLABORATOR
                    elif 'label' in value:
                        column_type = ColumnTypes.BUTTON
                    elif 'text' in value:
                        column_type = ColumnTypes.BARCODE
                #
                type_list.append(column_type)
            column_type = max(
                type_list, key=type_list.count) if type_list else ColumnTypes.TEXT
        except Exception as e:
            print('[Error] get column type error:', e)
        return column_type

    def gen_columns(self, link_map, table_name, value_map):
        columns = []
        for column_name, values in value_map.items():
            if column_name in ('_id', 'Name', '名称'):
                continue
            column_type = self.get_column_type(values)
            column_type, column_data = self.get_column_data(
                link_map, table_name, column_name, column_type, values)
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
            row = record['fields']
            row['_id'] = record['id']
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

    def __init__(self, airtable_api_key, airtable_base_id, base, table_names, links=[]):
        """
        airtable_api_key: str
        airtable_base_id: str
        base: SeaTable Base
        table_names: list[str], eg: ['table_name1', 'table_name2']
        links: list[tuple], eg: [('table_name', 'column_name', 'other_table_name')]
        """
        self.airtable_api = AirtableAPI(airtable_api_key, airtable_base_id)
        self.base = base
        self.table_names = table_names
        self.links = links
        self.columns_parser = ColumnsParser()
        self.rows_convertor = RowsConvertor()
        self.links_convertor = LinksConvertor()
        self.parse_columns_by_all_rows = False
        self.airtable_row_map = {}
        self.get_link_map()

    def auto_convert(self):
        self.convert_tables()
        self.convert_columns()
        self.convert_rows()
        self.convert_links()

    def convert_tables(self):
        self.get_table_map()
        for table_name in self.table_names:
            table = self.table_map.get(table_name)
            if not table:
                self.add_table(table_name)
                print('[Info] Added table [ %s ]' % table_name)
        time.sleep(1)
        print('[Info] Convert tables success\n')

    def convert_columns(self):
        self.get_table_map()
        for table_name in self.table_names:
            if self.parse_columns_by_all_rows:
                airtable_rows = self.airtable_api.list_all_rows(table_name)
            else:
                airtable_rows, offset = self.airtable_api.list_rows(table_name)
            columns = self.columns_parser.parse(
                self.link_map, table_name, airtable_rows)
            table = self.table_map.get(table_name)
            for column in columns:
                column_name = column['name']
                exists_column = table.get(column_name)
                if not exists_column:
                    self.add_column(
                        table_name, column_name, column['type'], column['data'])
                    print(
                        '[Info] Added column [ %s ] to table <%s>' % (column['name'], table_name))
            time.sleep(1)
        print('[Info] Convert columns success\n')

    def convert_rows(self):
        self.get_airtable_row_map()
        for table_name in self.table_names:
            airtable_rows = self.airtable_row_map[table_name]
            columns = self.list_columns(table_name)
            rows = self.rows_convertor.convert(columns, airtable_rows)
            self.batch_append_rows(table_name, rows)
            time.sleep(1)
        print('[Info] Convert rows success\n')

    def convert_links(self):
        if not self.links:
            return
        self.get_airtable_row_map()
        self.get_table_map()
        for table_name, column_names in self.link_map.items():
            airtable_rows = self.airtable_row_map[table_name]
            links = self.links_convertor.convert(
                self.tables, columns, airtable_rows)
            self.batch_append_links(table_name, links)
            time.sleep(1)
        print('[Info] Convert links success\n')

    def get_link_map(self):
        self.link_map = {}
        for link in self.links:
            table_name = link[0]
            column_name = link[1]
            if not self.link_map.get(table_name):
                self.link_map[table_name] = {}
            self.link_map[table_name][column_name] = link
        return self.link_map

    def get_airtable_row_map(self):
        if not self.airtable_row_map:
            self.airtable_row_map = {}
            for table_name in self.table_names:
                rows = self.airtable_api.list_all_rows(table_name)
                self.airtable_row_map[table_name] = rows
        return self.airtable_row_map

    def get_table_map(self):
        metadata = self.base.get_metadata()
        self.tables = metadata['tables']
        self.table_map = {}
        for table in self.tables:
            table_name = table['name']
            self.table_map[table_name] = {
                '_id': table['_id'],
                'name': table_name,
            }
            for column in table['columns']:
                column_name = column['name']
                self.table_map[table_name][column_name] = column
        return self.table_map

    def add_table(self, table_name):
        self.base.add_table(table_name)

    def add_column(self, table_name, column_name, column_type, column_data):
        try:
            self.base.insert_column(
                table_name, column_name, column_type, None, column_data)
        except Exception as e:
            print(e)

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

    def batch_append_links(self, table_name, rows):
        self.base.batch_update_links(
            link_id, table_id, other_table_id, row_id_list, other_rows_ids_map)
