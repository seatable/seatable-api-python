from datetime import datetime, timedelta

import socketio

from .constants import JOIN_ROOM, UPDATE_DTABLE, NEW_NOTIFICATION


class SocketIO(object):

    def __init__(self, base):
        self.base = base
        self.jwt_exp = datetime.now() + timedelta(days=3)
        self.sio = socketio.Client(request_timeout=self.base.timeout)
        self.sio_init()

    def __str__(self):
        return '<SeaTable SocketIO [ %s ]>' % self.base.dtable_name

    def sio_init(self):
        self.sio.on('connect', self.on_connect)
        self.sio.on('connect_error', self.on_connect_error)
        self.sio.on('disconnect', self.on_disconnect)
        self.sio.on(UPDATE_DTABLE, self.on_update_dtable)
        self.sio.on(NEW_NOTIFICATION, self.on_new_notification)

        url = self.base.dtable_server_url + '?dtable_uuid=' + self.base.dtable_uuid
        self.sio.connect(url)

    def refresh_jwt_token(self):
        if datetime.now() >= self.jwt_exp:
            print('%s [ Seatable Socket IO Refresh JWT Token ]' % datetime.now())
            self.jwt_exp = datetime.now() + timedelta(days=3)
            self.base.auth()

    def on_connect(self):
        self.refresh_jwt_token()
        self.sio.emit(JOIN_ROOM, (self.base.dtable_uuid, self.base.jwt_token))
        print('%s [ Seatable Socket IO Connected ]' % datetime.now())

    def on_connect_error(self, error_msg):
        print('%s [ Seatable Socket IO Connect Error ]' % datetime.now())
        print('Error:', error_msg)

    def on_disconnect(self):
        print('%s [ Seatable Socket IO Disconnected ]' % datetime.now())

    def on_update_dtable(self, data, index, *args):
        """ Default is print received data
            You can overwrite this event
        """
        print('%s [ Seatable Socket IO UPDATE DTABLE ]' % datetime.now())
        print(data)

    def on_new_notification(self, data, index, *args):
        """ Default is print received data
            You can overwrite this event
        """
        print('%s [ Seatable Socket IO NEW NOTIFICATION ]' % datetime.now())
        print(data)

    def on(self, event, handler):
        self.sio.on(event, handler)

    def wait(self):
        self.sio.wait()
