from __future__ import absolute_import, division, print_function

import functools
import logging

from distutils.version import LooseVersion

import pymongo
import pymongo.common as common

from bson.py3compat import string_type
from pymongo import MongoClient
from pymongo.client_options import ClientOptions
from pymongo.uri_parser import parse_uri, split_hosts

from .selectors import SERVER_STATE_SELECTORS, SERVER_TYPE_SELECTORS
from .monitoring import ServerHeartbeatListener


logger = logging.getLogger(__name__)


def mongo_client(*args, **kwargs):
    """ MongoClient factory supporting instrumentation for fast-fail behavior.

    All parameters are the same as those you would pass to ``MongoClient``, with
    the addition of the following keyword arguments:

    :param bool fail_fast: provide "fail fast" semantics on connect (default
                           is True)
    :param list state_selectors: list of server state selectors (see:
                                 ``SERVER_STATE_SELECTORS``)
    :param list type_selectors: list of server type selectors (see:
                                ``SERVER_TYPE_SELECTORS``)
    :returns: An instance of ``MongoClient``
    :raises: ``errors.ServerSelectionTryOnceTimeoutError``

    *NOTE*: currently, ``fail_fast==True`` implies ``connect==True``

    """
    # MongoClient default parameters
    _args = ('host', 'port', 'document_class', 'tz_aware', 'connect')
    _kwargs = dict(zip(_args, MongoClient.__init__.func_defaults))
    # update default parameters with positional args if they were passed
    for i, arg in enumerate(args):
        _kwargs[_args[i]] = args[i]

    # grab arguments to this factory function
    fail_fast = kwargs.pop('fail_fast', True)
    state_selectors = kwargs.pop('state_selectors', None)
    type_selectors = kwargs.pop('type_selectors', None)

    # updated kwargs with default parameters
    for k, v in _kwargs.iteritems():
        kwargs[k] = v

    if fail_fast:
        # extract the seed list from the host argument
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

        # use pymongo to parse out connect_timeout
        client_options = \
            ClientOptions(
                None, None, None, 
                dict([common.validate(k, v)
                      for k, v in filter(lambda x: x[0] not in _args,
                                         kwargs.items())]))

        # create our event listener
        listener = \
            ServerHeartbeatListener(
                seeds, client_options.pool_options.connect_timeout,
                state_selectors, type_selectors)
        if 'event_listeners' not in kwargs:
            kwargs['event_listeners'] = []
        # add it to the list of listeners associated with 
        kwargs['event_listeners'].append(listener)
        if LooseVersion(pymongo.__version__) < LooseVersion('3.3'):
            # if we are on a version < 3.3 we have to deliver server heartbeat
            # events ourselves so we wrap the monitor class and add it to the
            # parameters being passed to MongoClient
            from .monitor import Monitor
            listener = kwargs['event_listeners'].pop()
            kwargs['_monitor_class'] = functools.partial(Monitor, listener)
        # XXX: always set connect == True if we are using "fail_fast", we
        #      should accommodate a lazy version of this down the road
        kwargs['connect'] = True
        # finally, create the client with our listener
        c = MongoClient(**kwargs)
        # wait for the seed list to update or throw an exception
        listener.wait()
        # there is at least one seed that is up and that satisfies at least on
        # of the server selectors specified
        return c

    # if fail_fast is False, simply pass through arguments to MongoClient
    return MongoClient(**kwargs)

