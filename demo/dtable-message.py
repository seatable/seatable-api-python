from seatable_api import SeaTableAPI, Base

server_url = "http://127.0.0.1:8000"
api_token = "aedec9158d95d8edb27bc3a6613888a9bf3d0ce8"

base = Base(api_token, server_url)
base.auth(
    # msg_sender_account="My Email Account"
)
print('sending.....')
base.send_msg('api 发送消息测试1',
              using_account="My Email Account",
              subject="Test",
              send_to='350178982@qq.com',
              copy_to="r350178982@126.com",
              # quit_after_send=True
              )
base.send_msg('api 发送消息测试2',
              # using_account="My wechat group",
              subject="Test",
              send_to=['350178982@qq.com',"r350178982@126.com"]
              )
base.send_msg('api 发送消息测试3',
              # using_account="My Email Account",
              subject="Test",
              send_to='350178982@qq.com'
              )
base.send_msg('api 发送消息测试4',
              using_account="My Email Account",
              subject="Test",
              send_to=['350178982@qq.com',"r350178982@126.com"],
              copy_to='jiwei_ran@sina.com'
              )
base.msg_quit()
