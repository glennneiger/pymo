import logging
import time

from pymongo.monitor import Monitor as _Monitor


logger = logging.getLogger(__name__)


class ServerHeartbeatEvent(object):
    def __init__(self, connection_id, duration, reply):
        self.connection_id = connection_id
        self.duration = duration
        self.reply = reply


class Monitor(_Monitor):
    def __init__(self, listener, *args, **kwargs):
        super(Monitor, self).__init__(*args, **kwargs)
        self._listener = listener

    def open(self):
        super(Monitor, self).open()

    def _check_once(self, *args, **kwargs):
        logger.debug('_check_once called')
        sd = None
        try:
            then = time.time()
            sd = super(Monitor, self)._check_once(*args, **kwargs)
        except Exception as e:
            self._listener.failed(
                ServerHeartbeatEvent(
                    self._server_description.address, time.time() - then, e))
            raise
        return sd

    def _check_with_socket(self, *args, **kwargs):
        logger.debug('_check_with_socket called')
        ismaster, duration = super(Monitor, self)._check_with_socket(*args, **kwargs)
        self._listener.succeeded(
            ServerHeartbeatEvent(
                self._server_description.address, duration, ismaster))
        return (ismaster, duration)

