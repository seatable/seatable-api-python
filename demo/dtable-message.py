from seatable_api import SeaTableAPI, Base

server_url = "http://127.0.0.1:8000"
api_token = "aedec9158d95d8edb27bc3a6613888a9bf3d0ce8"

base = Base(api_token, server_url)
# base.auth()
base.send_msg('api 发送消息测试',
              # using_account="My Email Account",
              subject="Test",
              email='350178982@qq.com'
              )
# base.send_msg()