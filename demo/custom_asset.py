from seatable_api import Base
from seatable_api.main import parse_headers, parse_response
api_token = "0935ffa1bd82814579240127472bd3a3e89a3b4c"
server_url = "http://127.0.0.1:8000"
import requests



base = Base(api_token, server_url)

base.auth()

path = '/'
name = '02.png'

# res = base.get_custom_file_download_link('/main/02.png')
# print(res)

# res_info = base.get_custom_file_info(path, '3.4.0.sql')
# print(res_info)

# res_upload_link = base.get_custom_file_upload_link(path)
# print(res_upload_link)

# res = base.upload_local_file_to_custom_folder('/Users/ranjiwei/Desktop/dtable.json', path)
# print(res)

# base.append_row('Table1', {'File': [res,]})

res = base.list_custom_assets(path)
print(res)




