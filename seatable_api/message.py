import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import requests

class EmailSender(object):

    def __init__(self, detail):
        self.detail = detail
        self.sender = None
        self._get_server_connection()

    def _get_server_connection(self):
        email_host = self.detail.get('email_host')
        email_port = int(self.detail.get('email_port'))
        host_user = self.detail.get('host_user')
        password = self.detail.get('password')
        smtp = smtplib.SMTP(email_host, email_port)
        smtp.starttls()
        smtp.login(host_user, password)
        self.sender = host_user
        self._email_server = smtp

    def send_msg(self, msg, **kwargs,):
        msg_obj = MIMEMultipart()
        content_body = MIMEText(msg)
        contact_email = kwargs.get('email', '')
        subject = kwargs.get('subject', '')
        source = kwargs.get('from', '')
        copy_to = kwargs.get('cc', '')
        reply_to = kwargs.get('reply_to')
        if not contact_email:
            raise ValueError('The email is not valid in the email sender')
        if not subject:
            raise ValueError('The subject is not valid in the email sender')

        msg_obj['Subject'] = subject
        msg_obj['From'] = source
        msg_obj['To'] = contact_email
        msg_obj['Cc'] = copy_to
        msg_obj['Reply-to'] = reply_to
        msg_obj.attach(content_body)
        server = self._email_server
        server.sendmail(self.sender, contact_email, msg_obj.as_string())
        quit_after_send = kwargs.get('quit_after_send', False)
        if quit_after_send:
            server.quit()
            self._email_server = None

class WechatSender(object):

    def __init__(self, detail):
        self.detail = detail

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

    def send_msg(self, msg, **kwargs):
        webhook_url = self.detail.get('webhook_url')
        requests.post(webhook_url, json=self._format_msg(msg), headers=self._headers)


def get_sender_by_account(account):

    account_type = account.get('account_type', '')
    detail = account.get('detail')
    if account_type == 'email':
        return EmailSender(detail)
    if account_type == 'wechat_robot':
        return WechatSender(detail)
