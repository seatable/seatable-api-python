from seatable_api import Base
from seatable_api.constants import UPDATE_DTABLE, NEW_NOTIFICATION
from seatable_api.utils import convert_row

server_url = 'http://127.0.0.1:8000'
api_token = '678cdf2deba6e2abf5dc354938b717c45239629b'


def on_update_seatable(data, index, *args):
    """ You can overwrite this event
    """
    row = convert_row(metadata, data)
    print(row)


def on_new_notification(data, index, *args):
    """ You can overwrite this event
    """
    print(data)


def connect_socket_io():

    base = Base(api_token, server_url)
    base.auth(with_socket_io=True)

    global metadata
    metadata = base.get_metadata()

    # overwrite events
    base.socketIO.on(UPDATE_DTABLE, on_update_seatable)
    base.socketIO.on(NEW_NOTIFICATION, on_new_notification)

    base.socketIO.wait()


if __name__ == '__main__':
    connect_socket_io()
