import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import requests
import abc

class EmailSender(object):

    def __init__(self, detail):
        self.detail = detail
        self.sender = None
        self.msg_type = 'email'

    def _get_server_connection(self):

        email_host = self.detail.get('email_host')
        email_port = int(self.detail.get('email_port'))
        host_user = self.detail.get('host_user')
        password = self.detail.get('password')
        self.sender = host_user
        try:
            smtp = smtplib.SMTP(email_host, email_port)
            smtp.starttls()
            smtp.login(host_user, password)
        except:
            smtp = None
        return smtp

    def send_msg(self, msg, **kwargs):
        smtp_server = self._get_server_connection()
        if not smtp_server:
            raise ValueError("The connection to the server of email sender failed")
        msg_obj = MIMEMultipart()
        content_body = MIMEText(msg)
        send_to = kwargs.get('send_to', '')
        subject = kwargs.get('subject', '')
        source = kwargs.get('from', '')
        copy_to = kwargs.get('copy_to', '')
        reply_to = kwargs.get('reply_to')
        if not send_to:
            raise ValueError('The email is not valid in the email sender')
        if not subject:
            raise ValueError('The subject is not valid in the email sender')

        if not isinstance(send_to, list):
            send_to = [send_to, ]

        if not isinstance(copy_to, list):
            copy_to = [copy_to, ]
        if not source:
            source = self.sender
        msg_obj['Subject'] = subject
        msg_obj['From'] = source
        msg_obj['To'] = ",".join(send_to)
        msg_obj['Cc'] = copy_to and ",".join(copy_to) or ""
        msg_obj['Reply-to'] = reply_to
        msg_obj.attach(content_body)
        recevers = copy_to and send_to + copy_to or send_to
        smtp_server.sendmail(self.sender, recevers, msg_obj.as_string())
        smtp_server.quit()

class WechatSender(object):

    def __init__(self, detail):
        self.detail = detail
        self.msg_type = 'wechat'

    @property
    def _headers(self):

        return {
            "Content-Type": "application/json"
        }

    def _format_msg(self, msg):
        return {
            "msgtype": "text",
            "text": {
                "content": msg,
            }
    }

    def send_msg(self, msg):
        webhook_url = self.detail.get('webhook_url')
        requests.post(webhook_url, json=self._format_msg(msg), headers=self._headers)

def get_sender_by_account(account):

    account_type = account.get('account_type', '')
    detail = account.get('detail')
    if account_type == 'email':
        return EmailSender(detail)
    if account_type == 'wechat_robot':
        return WechatSender(detail)
