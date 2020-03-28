from urllib.parse import splitport
from socketIO_client_nexus import SocketIO, BaseNamespace

from .constants import JOIN_ROOM, UPDATE_DTABLE, NEW_NOTIFICATION


def parse_dtable_server_url(dtable_server_url):
    host, port = splitport(dtable_server_url)
    return host, port


class SeatableNamespace(BaseNamespace):

    def on_connect(self):
        print('[ Seatable Socket IO Connected ]')

    def on_reconnect(self):
        print('[ Seatable Socket IO Reconnected ]')

    def on_disconnect(self):
        print('[ Seatable Socket IO Disconnected ]')

    def on_event(self, event, data, index, *args):
        if event not in [JOIN_ROOM, UPDATE_DTABLE, NEW_NOTIFICATION]:
            print(f'[ {event} ] : {data}')
        else:
            print(f'[ {event} ] : {data}')
            return event, data


def connect_socket_io(dtable_server_url, dtable_uuid, jwt_token):
    host, port = parse_dtable_server_url(dtable_server_url)
    params = {'dtable_uuid': dtable_uuid}

    socketIO = SocketIO(host, port, SeatableNamespace, params=params)
    socketIO.emit(JOIN_ROOM, dtable_uuid, jwt_token)
    print('[ Seatable Socket IO Connected ]')

    return socketIO
