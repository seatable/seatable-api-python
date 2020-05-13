import os
import sys
import json
import requests
from datetime import datetime

from seatable_api import SeaTableAPI


server_url = 'http://127.0.0.1:8000'
api_token = 'd67d4e0eeee24b55ff7b60595faed7e2df36e1d1'


def upload_file():

    seatable = SeaTableAPI(api_token, server_url)
    seatable.auth()

    table_name = 'Table1'
    file_name = 'demo.zip'
    file_path = '/tmp/demo.zip'
    file_column_name = '文件'
    WORKSPACE_ID = 9

    relative_path = 'files/' + datetime.now().strftime('%Y-%m')

    rows = seatable.list_rows(table_name)
    row = rows[0]
    row_id = row['_id']

    upload_url_data = seatable.get_file_upload_link()
    upload_url = upload_url_data.get('upload_link') + '?ret-json=1'
    parent_dir = upload_url_data.get('parent_path')

    data = {
        'parent_dir': parent_dir,
        'relative_path': relative_path,
    }
    files = {'file': (file_name, open(file_path, 'rb'))}
    resp = requests.post(upload_url, data=data, files=files)

    resp_json = json.loads(resp.text)
    name = resp_json[0]['name']
    size = resp_json[0]['size']
    url = server_url.rstrip('/') + '/workspace/' + str(WORKSPACE_ID) + \
        parent_dir + '/' + relative_path + '/' + name

    file_data = {
        'name': name,
        'size': size,
        'type': 'file',
        'url': url,
    }

    files_data = row.get(file_column_name) or []
    files_data.append(file_data)

    row_data = {
        file_column_name: files_data,
    }
    seatable.update_row(table_name, row_id, row_data)


if __name__ == '__main__':
    upload_file()
