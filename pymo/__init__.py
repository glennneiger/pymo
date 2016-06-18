import functools
import logging
import threading

from collections import namedtuple
from distutils.version import LooseVersion

import pymongo
import pymongo.common as common

from bson.py3compat import string_type
from pymongo import MongoClient
from pymongo.client_options import ClientOptions
from pymongo.errors import ServerSelectionTimeoutError
try:
    from pymongo.monitoring import (
        ServerHeartbeatListener as _ServerHeartbeatListener,
    )
except ImportError:
    _ServerHeartbeatListener = object
from pymongo.server_description import ServerDescription
from pymongo.server_type import SERVER_TYPE
from pymongo.uri_parser import parse_uri, split_hosts


logger = logging.getLogger(__name__)


SERVER_STATE_SELECTORS = \
    namedtuple('ServerStateType', ('Writable', 'Readable'))(*range(2))
SERVER_TYPE_SELECTORS = SERVER_TYPE


class ServerSelectionTryOnceTimeoutError(ServerSelectionTimeoutError):
    pass


class ServerHeartbeatListener(_ServerHeartbeatListener):
    def __init__(self, seeds, connect_timeout, state_selectors=None,
                       type_selectors=None):
        self._seeds = seeds
        self._connect_timeout = connect_timeout
        self._state_selectors = [] if state_selectors is None \
                                   else state_selectors
        if isinstance(self._state_selectors, string_type):
            self._state_selectors = [self._state_selectors]
        self._type_selectors = [] if type_selectors is None \
                                  else type_selectors
        if isinstance(self._type_selectors, string_type):
            self._type_selectors = [self._type_selectors]

        self._lock = threading.Lock()
        self._heartbeats = {}
        self._done = threading.Event()
        self._match = False
        self._error = \
            ServerSelectionTryOnceTimeoutError(
                'Timed out waiting for an acceptable server response')

    def _check_selectors(self):
        all_seeds_collected = \
            all([seed in self._heartbeats for seed in self._seeds])

        # no selectors, check for errors if all seeds collected
        if len(self._state_selectors) == 0 and \
           len(self._type_selectors) == 0 and \
           all_seeds_collected and \
           not any(['error' in v for v in self._heartbeats.values()]):
            self._match = True
            self._done.set()

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

        # finally, check if all seeds are accounted for, and if there is at
        # least one error, raise an exception
        if all_seeds_collected:
            errors = [r['error'] for r in self._heartbeats.values() if 'error' in r]
            if len(errors) > 0:
                # XXX: figure out how this should actually look
                self._error = \
                    ServerSelectionTryOnceTimeoutError(repr(errors))
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


def mongo_client(*args, **kwargs):
    _args = ('host', 'port', 'document_class', 'tz_aware', 'connect')
    _kwargs = dict(zip(_args, MongoClient.__init__.func_defaults))
    for i, arg in enumerate(args):
        _kwargs[_args[i]] = args[i]

    fail_fast = kwargs.pop('fail_fast', True)
    state_selectors = kwargs.pop('state_selectors', None)
    type_selectors = kwargs.pop('type_selectors', None)

    for k, v in _kwargs.iteritems():
        kwargs[k] = v

    if fail_fast:
        seeds = set()
        if kwargs['host'] is None:
            kwargs['host'] = MongoClient.HOST
        if kwargs['port'] is None:
            kwargs['port'] = MongoClient.PORT
        if isinstance(kwargs['host'], string_type):
            kwargs['host'] = [kwargs['host']]
        for host in kwargs['host']:
            if '://' in host:
                if host.startswith('mongodb://'):
                    seeds.update(parse_uri(host, kwargs['port'])['nodelist'])
                else:
                    # let MongoClient raise the error
                    MongoClient(**kwargs)
            else:
                seeds.update(split_hosts(host, kwargs['port']))
        client_options = \
            ClientOptions(
                None, None, None, 
                dict([common.validate(k, v)
                      for k, v in filter(lambda x: x[0] not in _args,
                                         kwargs.items())]))
        listener = \
            ServerHeartbeatListener(
                seeds, client_options.pool_options.connect_timeout,
                state_selectors, type_selectors)
        if 'event_listeners' not in kwargs:
            kwargs['event_listeners'] = []
        kwargs['event_listeners'].append(listener)
        if LooseVersion(pymongo.__version__) < LooseVersion('3.3'):
            # no ServerHeartbeatListener till 3.3
            from .monitor import Monitor
            listener = kwargs['event_listeners'].pop()
            kwargs['_monitor_class'] = functools.partial(Monitor, listener)
        c = MongoClient(**kwargs)
        listener.wait()
        return c

    return MongoClient(**kwargs)

