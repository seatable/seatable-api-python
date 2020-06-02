from urllib.parse import splitport
from socketIO_client_nexus import SocketIO

from .constants import JOIN_ROOM, UPDATE_DTABLE, NEW_NOTIFICATION


def parse_dtable_server_url(dtable_server_url):
    host, port = splitport(dtable_server_url)
    return host, port


def on_connect():
    print('[ Seatable Socket IO Connected ]')


def on_reconnect():
    print('[ Seatable Socket IO Reconnected ]')


def on_disconnect():
    print('[ Seatable Socket IO Disconnected ]')


def on_update_dtable(data, index, *args):
    """ Default is print received data
        You can overwrite this event
    """
    print(UPDATE_DTABLE, data)


def on_new_notification(data, index, *args):
    """ Default is print received data
        You can overwrite this event
    """
    print(NEW_NOTIFICATION, data)


def connect_socket_io(dtable_server_url, dtable_uuid, jwt_token):
    host, port = parse_dtable_server_url(dtable_server_url)
    params = {'dtable_uuid': dtable_uuid}

    socketIO = SocketIO(host, port, params=params)
    socketIO.on('connect', on_connect)
    socketIO.on('reconnect', on_reconnect)
    socketIO.on('disconnect', on_disconnect)

    socketIO.emit(JOIN_ROOM, dtable_uuid, jwt_token)
    print('[ Seatable Socket IO Connected]')

    socketIO.on(UPDATE_DTABLE, on_update_dtable)
    socketIO.on(NEW_NOTIFICATION, on_new_notification)

    return socketIO
