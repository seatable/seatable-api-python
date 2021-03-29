from seatable_api import Base

server_url = "http://127.0.0.1:8000"
api_token = "aedec9158d95d8edb27bc3a6613888a9bf3d0ce8"

base = Base(api_token, server_url)
base.auth()
print('sending.....')
base.send_email("My Email Account",
              'api 发送消息测试1',
              subject="Test",
              send_to='350178982@qq.com',
              copy_to="r350178982@126.com",
              )

base.send_email('My Email Account',
                'api 发送消息测试2',
              subject="Test2",
              send_to=['350178982@qq.com',"r350178982@126.com"]
              )

base.send_wechat_msg("My wechat group", "Api 发送测试")

