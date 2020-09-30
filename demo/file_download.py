import os
import uuid
from urllib import parse
import requests
from seatable_api import Base


## replace server_url and api_token with your server address and api token 
server_url = 'https://cloud.seatable.io'
api_token = '62374a03996cc365a977d1310bf8e099189db312'

base = Base(api_token, server_url)

base.auth()

####### checkout all file urls #######

meta = base.get_metadata()

tables = meta.get('tables', [])

files = []

for table in tables:
    columns = table.get('columns', [])
    # if you want to only download image just change following line to 
    # file_cols = [col for col in columns if col.get('type') in ('image',)]
    file_cols = [col for col in columns if col.get('type') in ('image', 'file')]

    rows = base.list_rows(table.get('name'))
    if not rows:
        continue

    for row in rows:
        for col in file_cols:
            if col.get('type') == 'image':
                files.extend(row.get(col.get('name'), []))
            elif col.get('type') == 'file':
                col_files = row.get(col.get('name'), [])
                files.extend([c_file.get('url') for c_file in col_files])

print('total check images and files: ', len(files))

####### confirm dir to save #######

dir_name = 'files'
no = 0
while True:
    if no == 0:
        if not os.path.isdir(dir_name):
            break
    else:
        if not os.path.isdir(dir_name + ' (%s)' % (no,)):
            break
    no += 1

dir_name = dir_name if no == 0 else dir_name + ' (%s)' % (no,)

####### download and save files one by one #######

for index, file in enumerate(files, start=1):
    print('start to download %s file/image' % (index,))
    if str(uuid.UUID(base.dtable_uuid)) in file:
        try:
            if '/images/' in file:
                path = parse.unquote(file[file.find('/images/'):])
            elif '/files/' in file:
                path = parse.unquote(file[file.find('/files/'):])
            else:
                print('fail to download %s file, url is invalid.' % (index,))
                continue
            file_url = base.get_file_download_link(path)
            file_name = file_url.split('/')[-1]
        except Exception as e:
            print(e)
            print('fail to download %s file' % (index,))
            continue
    else:
        # other non-uploaded file
        file_url = file
        file_name = file_url.split('?')[0].split('/')[-1]

    try:
        response = requests.get(file_url)
    except Exception as e:
        print('fail to download %s file/image' % (index,))
        continue
    if response.status_code != 200:
        print('fail to download %s file/image' % (index,))
        continue

    if not os.path.isdir(dir_name):
        os.mkdir(dir_name)

    file_name = parse.unquote(file_name)
    with open(os.path.join(dir_name, file_name), 'wb') as f:
        f.write(response.content)

    print('success to download %s file/image' % (index,))
