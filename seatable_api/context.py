import os
import sys
import json
import functools


is_cloud = os.environ.get('is_cloud', "0") == "1"
if is_cloud and not sys.stdin.isatty():
    context_data = sys.stdin.read()
    try:
        context_data = json.loads(context_data)
    except:
        context_data = None
else:
    context_data = None


def need_data(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if not context_data:
            return None
        return func(*args, **kwargs)
    return wrapper


class Context:
    @property
    def server_url(self):
        return os.environ.get('dtable_web_url')

    @property
    def api_token(self):
        return os.environ.get('api_token')

    @property
    @need_data
    def current_row(self):
        return context_data.get('row')

    @property
    @need_data
    def current_table(self):
        return context_data.get('table')

    @property
    @need_data
    def current_user_id(self):
        return context_data.get('current_user_id')

    @property
    @need_data
    def current_username(self):
        return context_data.get('current_username')

    @property
    @need_data
    def current_id_in_org(self):
        return context_data.get('current_id_in_org')

    @need_data
    def get_setting_by_key(self, key):
        return context_data.get(key)


context = Context()
