from __future__ import absolute_import, division, print_function

import logging
import threading

from bson.py3compat import integer_types
try:
    from pymongo.monitoring import (
        ServerHeartbeatListener as _ServerHeartbeatListener,
    )
except ImportError:
    _ServerHeartbeatListener = object

from .errors import ServerSelectionTryOnceTimeoutError
from .selectors import SERVER_STATE_SELECTORS, SERVER_TYPE_SELECTORS


logger = logging.getLogger(__name__)


class ServerHeartbeatListener(_ServerHeartbeatListener):
    def __init__(self, seeds, connect_timeout, state_selectors=None,
                       type_selectors=None):
        """ ServerHeartbeatListener constructor

        :param list seeds: list of servers to listen for events on
        :param float connection_timeout: MongoClient connection timeout
        :param list state_selectors: server state selectors to match
        :param list type_selectors: server type selectors to match
        """
        self._seeds = seeds
        self._connect_timeout = connect_timeout
        self._state_selectors = [] if state_selectors is None \
                                   else state_selectors
        if isinstance(self._state_selectors, integer_types):
            self._state_selectors = [self._state_selectors]
        self._type_selectors = [] if type_selectors is None \
                                  else type_selectors
        if isinstance(self._type_selectors, integer_types):
            self._type_selectors = [self._type_selectors]

        self._lock = threading.Lock()
        self._heartbeats = {}
        self._done = threading.Event()
        self._match = False
        self._error = \
            ServerSelectionTryOnceTimeoutError(
                'Timed out waiting for an acceptable server response')

    def _check_selectors(self):
        # we've received a event for all seeds
        all_seeds_collected = \
            all([seed in self._heartbeats for seed in self._seeds])
        errors = [r['error'] for r in self._heartbeats.values() if 'error' in r]

        if len(self._state_selectors) == 0 and \
           len(self._type_selectors) == 0 and \
           all_seeds_collected:
            # no selectors, check for errors if all seeds collected
            if any(errors):
                self._error = \
                    ServerSelectionTryOnceTimeoutError(repr(errors))
            else:
                self._match = True
            self._done.set()
        else:
            # check if there is a seed that satisfies one of the selectors
            for seed in self._heartbeats:
                if self._heartbeats[seed].get('error') is not None:
                    continue
                ismaster = self._heartbeats[seed]['ismaster']
                if SERVER_STATE_SELECTORS.Writable in self._state_selectors and \
                   ismaster.is_writable() or \
                   SERVER_STATE_SELECTORS.Readable in self._state_selectors and \
                   ismaster.is_readable():
                    logger.debug(
                        'seed {} matched a state_selector ({!r})'.format(
                            seed, self._state_selectors))
                    self._match = True
                    self._done.set()
                    break
                if ismaster.server_type in self._type_selectors:
                    logger.debug(
                        'seed {} matched a type_selector ({!r})'.format(
                            seed, self._type_selectors))
                    self._match = True
                    self._done.set()
                    break
            if all_seeds_collected and not self._match:
                # all seeds collected and no selectors satisfied, so exit
                self._error = \
                    ServerSelectionTryOnceTimeoutError('No selectors matched')
                self._done.set()

    def wait(self):
        self._done.wait(self._connect_timeout)
        # set to ensure this listener is disabled in the future
        self._done.set()
        # one final check
        with self._lock:
            self._check_selectors()
        # if there was no match, raise exception
        if not self._match:
            raise self._error

    def started(self, event):
        pass

    def succeeded(self, event):
        if self._done.is_set():
            return
        logger.debug('succeeded called with {!r}'.format(event.reply))
        try:
            with self._lock:
                if self._done.is_set() or event.connection_id not in self._seeds:
                    return
                self._heartbeats[event.connection_id] = {
                    'ismaster': event.reply
                }
                self._check_selectors()
        except Exception as e:
            logger.exception()

    def failed(self, event):
        if self._done.is_set():
            return
        logger.debug('failed called with {!r}'.format(event.reply))
        try:
            with self._lock:
                if self._done.is_set() or event.connection_id not in self._seeds:
                    return
                self._heartbeats[event.connection_id] = {
                    'error': event.reply
                }
                self._check_selectors()
        except Exception as e:
            logger.exception()

