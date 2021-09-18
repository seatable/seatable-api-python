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
TEXT_COLOR = '#FFFFFF'
DATE_FORMAT = '%Y-%m-%d'
DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'
FIRST_COLUMN_TYPES = [
    ColumnTypes.TEXT, ColumnTypes.NUMBER, ColumnTypes.DATE, ColumnTypes.SINGLE_SELECT, ColumnTypes.AUTO_NUMBER]
AIRTABLE_API_URL = 'https://api.airtable.com/v0/'
ColumnTypes.BARCODE = 'barcode'


class LinksConvertor(object):

    def convert(self, column_name, link_data, airtable_rows):
        links = self.gen_links(column_name, link_data, airtable_rows)
        return links

    def gen_links(self, column_name, link_data, airtable_rows):
        row_id_list = []
        other_rows_ids_map = {}
        try:
            for row in airtable_rows:
                row_id = row['_id']
                link = row.get(column_name)
                if not link:
                    continue
                row_id_list.append(row_id)
                other_rows_ids_map[row_id] = link
        except Exception as e:
            print('[Warning] gen links error:', e)
        links = {
            'link_id': link_data['link_id'],
            'table_id': link_data['table_id'],
            'other_table_id': link_data['other_table_id'],
            'row_id_list': row_id_list,
            'other_rows_ids_map': other_rows_ids_map,
        }
        return links


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

    def parse_image(self, value):
        return [item['url'] for item in value]

    def parse_file(self, value):
        return [{
                'name': item['filename'],
                'size': item['size'],
                'type': 'file',
                'url': item['url'],
                } for item in value]

    def parse_single_select(self, value):
        if isinstance(value, dict) and 'name' in value:
            return value['name']  # collaborator
        value = str(value)
        return value

    def parse_multiple_select(self, value):
        if isinstance(value[0], dict):
            collaborators = []
            for item in value:
                collaborators.append(item['name'])
            return collaborators  # collaborators
        else:
            return [str(item) for item in value]

    def parse_text(self, value):
        if isinstance(value, dict) and 'text' in value:
            return value['text']  # barcode
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
            elif column_type == (ColumnTypes.NUMBER, ColumnTypes.DURATION, ColumnTypes.RATE):
                cell_data = value
            elif column_type in (ColumnTypes.URL, ColumnTypes.EMAIL):
                cell_data = str(value)
            elif column_type == ColumnTypes.TEXT:
                cell_data = self.parse_text(value)
            elif column_type == ColumnTypes.LONG_TEXT:
                cell_data = self.parse_long_text(value)
            elif column_type == ColumnTypes.DATE:
                cell_data = self.parse_date(value)
            elif column_type == ColumnTypes.FILE:
                cell_data = self.parse_file(value)
            elif column_type == ColumnTypes.IMAGE:
                cell_data = self.parse_image(value)
            elif column_type == ColumnTypes.SINGLE_SELECT:
                cell_data = self.parse_single_select(value)
            elif column_type == ColumnTypes.MULTIPLE_SELECT:
                cell_data = self.parse_multiple_select(value)
            elif column_type == ColumnTypes.LINK:
                cell_data = None
            elif column_type == ColumnTypes.BUTTON:
                cell_data = None
            elif column_type == ColumnTypes.GEOLOCATION:
                cell_data = None
            elif column_type in (ColumnTypes.COLLABORATOR, ColumnTypes.CREATOR, ColumnTypes.LAST_MODIFIER):
                cell_data = None
            elif column_type in (ColumnTypes.CTIME, ColumnTypes.MTIME):
                cell_data = None
            elif column_type in (ColumnTypes.FORMULA, ColumnTypes.LINK_FORMULA, ColumnTypes.AUTO_NUMBER):
                cell_data = None
            else:  # DEFAULT
                cell_data = str(value)
        except Exception as e:
            print('[Warning] gen cell data error:', e)
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
    """Airtable field types
    https://support.airtable.com/hc/en-us/articles/360055885353-Field-types-reference
    https://airtable.com/api/meta
    """

    def parse(self, link_map, table_name, airtable_rows):
        value_map = self.get_value_map(airtable_rows)
        columns = self.gen_columns(link_map, table_name, value_map)
        return columns

    def parse_select(self, columns, airtable_rows):
        select_value_map = self.get_select_value_map(columns, airtable_rows)
        select_columns = self.gen_select_columns(select_value_map)
        return select_columns

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

    def get_value_map(self, airtable_rows):
        value_map = {}
        for row in airtable_rows:
            for column_name, value in row.items():
                if column_name not in value_map:
                    value_map[column_name] = []
                if value is not None:
                    value_map[column_name].append(value)
        return value_map

    def get_select_options(self, select_list):
        return [{
            'name': str(value),
            'id': self.random_num_id(),
            'color': self.random_color(),
            'textColor': TEXT_COLOR,
        } for value in select_list]

    def get_column_data(self, link_map, table_name, column_name, column_type, values):
        column_data = None
        try:
            if column_type == ColumnTypes.BARCODE:
                column_type = ColumnTypes.TEXT
            elif column_type == ColumnTypes.DATE:
                column_data = {'format': 'YYYY-MM-DD'}
            elif column_type == ColumnTypes.LINK:
                other_table_name = link_map[table_name][column_name]
                column_data = {'other_table': other_table_name}
            elif column_type == ColumnTypes.MULTIPLE_SELECT:
                select_list = []
                for value in values:
                    for item in value:
                        item = str(item)
                        if item not in select_list:
                            select_list.append(item)
                column_data = {'options': self.get_select_options(select_list)}
            elif column_type == ColumnTypes.COLLABORATOR:
                if isinstance(values[0], dict):
                    column_type = ColumnTypes.SINGLE_SELECT
                    select_list = []
                    for value in values:
                        name = value['name']
                        if name not in select_list:
                            select_list.append(name)
                    column_data = {'options': self.get_select_options(select_list)}
                elif isinstance(values[0], list):
                    column_type = ColumnTypes.MULTIPLE_SELECT
                    select_list = []
                    for value in values:
                        for item in value:
                            name = item['name']
                            if name not in select_list:
                                select_list.append(name)
                    column_data = {'options': self.get_select_options(select_list)}
        except Exception as e:
            print('[Warning] get', column_type.value, 'column data error:', e)
        return column_type, column_data

    def get_column_type(self, values):
        column_type = ColumnTypes.TEXT
        try:
            type_list = []
            for value in values:
                # bool
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
                    elif isinstance(value[0], str) and value[0].startswith('rec'):
                        column_type = ColumnTypes.LINK
                    elif isinstance(value[0], dict):
                        if 'email' in value[0]:
                            column_type = ColumnTypes.COLLABORATOR
                        elif 'filename' in value[0]:
                            column_type = ColumnTypes.FILE
                    else:  # str, number, boole
                        column_type = ColumnTypes.MULTIPLE_SELECT
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
            print('[Warning] get column type error:', e)
        return column_type

    def gen_columns(self, link_map, table_name, value_map):
        columns = []
        for column_name, values in value_map.items():
            if column_name == '_id':
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

    # Select
    def get_select_value_map(self, columns, airtable_rows):
        select_value_map = {}
        for row in airtable_rows:
            for column_name, value in row.items():
                if column_name == '_id':
                    continue
                column = columns.get(column_name)
                if not column or not value:
                    continue
                column_type = ColumnTypes(column['type'])
                if column_type not in (ColumnTypes.SINGLE_SELECT, ColumnTypes.MULTIPLE_SELECT):
                    continue
                if column_name not in select_value_map:
                    select_value_map[column_name] = []
                items = []
                if isinstance(value, dict):
                    items.append(value.get('name', ''))  # collaborator
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            items.append(item.get('name', ''))  # collaborator
                        else:
                            items.append(item)
                else:
                    items.append(value)
                for item in items:
                    if item not in select_value_map[column_name]:
                        select_value_map[column_name].append(item)
        return select_value_map

    def gen_select_columns(self, select_value_map):
        select_columns = []
        for column_name, select_list in select_value_map.items():
            options = self.get_select_options(select_list)
            column = {
                'name': column_name,
                'options': options,
            }
            select_columns.append(column)
        return select_columns


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
                '[Info] Got [ %s ] rows in Airtable <%s>' % (len(all_rows), table_name))
            if not offset:
                break
            time.sleep(0.5)
        return all_rows


class AirtableConvertor(object):

    def __init__(self, airtable_api_key, airtable_base_id, base, table_names, first_columns=[], links=[]):
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
        self.first_columns = first_columns
        self.links = links
        self.columns_parser = ColumnsParser()
        self.rows_convertor = RowsConvertor()
        self.links_convertor = LinksConvertor()
        self.get_first_column_map()
        self.get_link_map()

    def convert_metadata(self):
        self.get_airtable_row_map(is_demo=True)
        self.get_airtable_column_map()
        self.convert_tables()
        self.convert_columns()
        self.convert_rows(is_demo=True)
        self.convert_links(is_demo=True)

    def convert_data(self):
        self.delete_demo_rows()
        self.get_airtable_row_map()
        self.convert_select_columns()
        self.convert_rows()
        self.convert_links()

    def convert_tables(self):
        print('[Info] Convert tables')
        self.get_table_map()
        for table_name in self.table_names:
            table = self.table_map.get(table_name)
            if not table:
                airtable_columns = self.airtable_column_map[table_name]
                first_column_name = self.first_column_map.get(table_name) or \
                    airtable_columns[0]['name']
                columns = []
                for column in airtable_columns:
                    if column['type'] == ColumnTypes.LINK:
                        continue
                    item = {
                        'column_name': column['name'],
                        'column_type': column['type'].value,
                        'column_data': column['data'],
                    }
                    if column['name'] == first_column_name:
                        if column['type'] not in FIRST_COLUMN_TYPES:
                            item['column_type'] = ColumnTypes.TEXT.value
                            item['column_data'] = None
                        columns.insert(0, item)
                    else:
                        columns.append(item)
                self.add_table(table_name, columns)
                print(
                    '[Info] Added table [ %s ] with %s columns' % (table_name, len(columns)))
        print('[Info] Success\n')
        time.sleep(1)

    def convert_columns(self):
        print('[Info] Convert columns')
        self.get_table_map()
        for table_name in self.table_names:
            airtable_columns = self.airtable_column_map[table_name]
            table = self.table_map.get(table_name)
            for column in airtable_columns:
                column_name = column['name']
                exists_column = table.get(column_name)
                if not exists_column:
                    self.add_column(
                        table_name, column_name, column['type'], column['data'])
                    print(
                        '[Info] Added column [ %s ] to table <%s>' % (column['name'], table_name))
        print('[Info] Success\n')
        time.sleep(1)

    def convert_rows(self, is_demo=False):
        print('[Info] Convert %s rows' % ('demo' if is_demo else ''))
        self.get_table_map()
        for table_name in self.table_names:
            airtable_rows = self.airtable_row_map[table_name]
            if is_demo:
                airtable_rows = airtable_rows[:10]
            columns = self.column_map[table_name]
            rows = self.rows_convertor.convert(columns, airtable_rows)
            self.batch_append_rows(table_name, rows)
        print('[Info] Success\n')
        time.sleep(1)

    def convert_links(self, is_demo=False):
        if not self.link_map:
            return
        print('[Info] Convert %s links' % ('demo' if is_demo else ''))
        self.get_table_map()
        for table_name, column_names in self.link_map.items():
            table = self.table_map[table_name]
            airtable_rows = self.airtable_row_map[table_name]
            if is_demo:
                airtable_rows = airtable_rows[:10]
            for column_name in column_names:
                link_data = table[column_name]['data']
                links = self.links_convertor.convert(
                    column_name, link_data, airtable_rows)
                self.batch_append_links(table_name, links)
        print('[Info] Success\n')
        time.sleep(1)

    def delete_demo_rows(self):
        print('[Info] Delete demo rows')
        for table_name in self.table_names:
            rows = self.list_rows(table_name)
            if rows:
                row_ids = [row['_id'] for row in rows]
                self.batch_delete_rows(table_name, row_ids)
        print('[Info] Success\n')
        time.sleep(1)

    def convert_select_columns(self):
        print('[Info] Convert select columns')
        self.get_table_map()
        for table_name in self.table_names:
            columns = self.table_map[table_name]
            airtable_rows = self.airtable_row_map[table_name]
            select_columns = self.columns_parser.parse_select(
                columns, airtable_rows)
            for column in select_columns:
                column_name = column['name']
                options = column['options']
                self.add_column_options(table_name, column_name, options)
                print(
                    '[Info] Added options to column [ %s ] in table <%s>' % (column_name, table_name))
        print('[Info] Success\n')
        time.sleep(1)

    def get_first_column_map(self):
        self.first_column_map = {}
        for column in self.first_columns:
            table_name = column[0]
            column_name = column[1]
            self.first_column_map[table_name] = column_name
        return self.first_column_map

    def get_link_map(self):
        self.link_map = {}
        for link in self.links:
            table_name = link[0]
            column_name = link[1]
            other_table_name = link[2]
            if not self.link_map.get(table_name):
                self.link_map[table_name] = {}
            self.link_map[table_name][column_name] = other_table_name
        return self.link_map

    def get_airtable_row_map(self, is_demo=False):
        print('[Info] List Airtable %s rows' % ('demo' if is_demo else ''))
        self.airtable_row_map = {}
        for table_name in self.table_names:
            if is_demo:
                rows, offset = self.airtable_api.list_rows(table_name)
            else:
                rows = self.airtable_api.list_all_rows(table_name)
            self.airtable_row_map[table_name] = rows
        print('[Info] Success\n')
        return self.airtable_row_map

    def get_airtable_column_map(self):
        self.airtable_column_map = {}
        for table_name in self.table_names:
            airtable_rows = self.airtable_row_map[table_name]
            columns = self.columns_parser.parse(
                self.link_map, table_name, airtable_rows)
            self.airtable_column_map[table_name] = columns
        return self.airtable_column_map

    def get_table_map(self):
        self.table_map = {}
        self.column_map = {}
        metadata = self.base.get_metadata()
        self.tables = metadata['tables']
        for table in self.tables:
            table_name = table['name']
            self.column_map[table_name] = table['columns']
            self.table_map[table_name] = {'_id': table['_id']}
            for column in table['columns']:
                column_name = column['name']
                self.table_map[table_name][column_name] = column
        time.sleep(0.1)
        return self.table_map

    def add_table(self, table_name, columns):
        table = self.base.add_table(table_name, columns=columns)
        time.sleep(0.1)
        return table

    def add_column(self, table_name, column_name, column_type, column_data):
        try:
            column = self.base.insert_column(
                table_name, column_name, column_type, None, column_data)
            time.sleep(0.1)
            return column
        except Exception as e:
            print(e)

    def add_column_options(self, table_name, column_name, options):
        column = self.base.add_column_options(table_name, column_name, options)
        time.sleep(0.1)
        return column

    def list_columns(self, table_name):
        columns = self.base.list_columns(table_name)
        time.sleep(0.1)
        return columns

    def list_rows(self, table_name):
        rows = self.base.list_rows(table_name)
        time.sleep(0.1)
        return rows

    def batch_delete_rows(self, table_name, row_ids):
        self.base.batch_delete_rows(table_name, row_ids)
        time.sleep(0.1)

    def batch_append_rows(self, table_name, rows):
        offset = 0
        while True:
            row_split = rows[offset: offset + LIMIT]
            offset = offset + LIMIT
            if not row_split:
                break
            self.base.batch_append_rows(table_name, row_split)
            print(
                '[Info] Appended [ %s ] rows to table <%s>' % (len(row_split), table_name))
            time.sleep(0.5)

    def batch_delete_rows(self, table_name, row_ids):
        offset = 0
        while True:
            row_id_split = row_ids[offset: offset + LIMIT]
            offset = offset + LIMIT
            if not row_id_split:
                break
            self.base.batch_delete_rows(table_name, row_id_split)
            print(
                '[Info] Deleted [ %s ] rows in table <%s>' % (len(row_id_split), table_name))
            time.sleep(0.5)

    def batch_append_links(self, table_name, links):
        link_id = links['link_id']
        table_id = links['table_id']
        other_table_id = links['other_table_id']
        row_id_list = links['row_id_list']
        other_rows_ids_map = links['other_rows_ids_map']
        offset = 0
        while True:
            row_id_split = row_id_list[offset: offset + LIMIT]
            offset = offset + LIMIT
            if not row_id_split:
                break
            other_rows_ids_map_split = {
                row_id: other_rows_ids_map[row_id] for row_id in row_id_split}
            self.base.batch_update_links(
                link_id, table_id, other_table_id, row_id_split, other_rows_ids_map_split)
            print(
                '[Info] Added [ %s ] Links to table <%s>' % (len(row_id_split), table_name))
            time.sleep(0.5)
