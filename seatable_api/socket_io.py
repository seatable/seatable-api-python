import time
from datetime import datetime

# https://python-socketio.readthedocs.io
import socketio

from .constants import JOIN_ROOM, UPDATE_DTABLE, NEW_NOTIFICATION


class SIO(socketio.Client):

    def _handle_disconnect(self, namespace):
        """io server disconnect"""
        self.logger.info('Engine.IO connection disconnected')
        if not self.connected:
            return
        self.disconnect()
        namespace = namespace or '/'
        self._trigger_event('io-disconnect', namespace=namespace)


class SocketIO(object):

    def __init__(self, base):
        self.base = base
        self.sio = SIO(request_timeout=base.timeout)

    def __str__(self):
        return '<SeaTable SocketIO [ %s ]>' % self.base.dtable_name

    def _connect(self):
        self.sio.on('connect', self._on_connect)
        self.sio.on('disconnect', self._on_disconnect)
        self.sio.on('io-disconnect', self._on_io_disconnect)
        self.sio.on('connect_error', self._on_connect_error)
        self.sio.on(UPDATE_DTABLE, self.on_update_dtable)
        self.sio.on(NEW_NOTIFICATION, self.on_new_notification)

        self.sio.connect(self._dtable_ws_url())

    def _dtable_ws_url(self):
        return self.base.dtable_server_url + '?dtable_uuid=' + self.base.dtable_uuid

    def _refresh_jwt_token(self):
        self.base.auth()
        print(datetime.now(), '[ SeaTable SocketIO JWT token refreshed ]')

    def _on_connect(self):
        if datetime.now() >= self.base.jwt_exp:
            self._refresh_jwt_token()
        self.sio.emit(JOIN_ROOM, (self.base.dtable_uuid, self.base.jwt_token))
        print(datetime.now(), '[ SeaTable SocketIO connection established ]')

    def _on_disconnect(self):
        print(datetime.now(), '[ SeaTable SocketIO connection dropped ]')

    def _on_io_disconnect(self):
        print(datetime.now(), '[ SeaTable SocketIO connection disconnected ]')
        time.sleep(3)
        self._refresh_jwt_token()
        self.sio.connect(self._dtable_ws_url())

    def _on_connect_error(self, error_msg):
        print(datetime.now(), '[ SeaTable SocketIO connection error ]', error_msg)

    def on_update_dtable(self, data, index, *args):
        """ Default is print received data
            You can overwrite this event
        """
        print(datetime.now(), '[ SeaTable SocketIO on UPDATE_DTABLE ]')
        print(data)

    def on_new_notification(self, data, index, *args):
        """ Default is print received data
            You can overwrite this event
        """
        print(datetime.now(), '[ SeaTable SocketIO on NEW_NOTIFICATION ]')
        print(data)

    def on(self, event, handler):
        self.sio.on(event, handler)

    def wait(self):
        self.sio.wait()
