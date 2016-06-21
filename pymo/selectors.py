from __future__ import absolute_import, division, print_function

from collections import namedtuple

from pymongo.server_type import SERVER_TYPE


SERVER_STATE_SELECTORS = \
    namedtuple('ServerStateType', ('Writable', 'Readable'))(*range(2))
""" selectors describing the state of a server

SERVER_STATE_SELECTORS.Writable
SERVER_STATE_SELECTORS.Readable
"""
SERVER_TYPE_SELECTORS = SERVER_TYPE
""" selectors describing the type of a server

SERVER_TYPE_SELECTORS.Unknown
SERVER_TYPE_SELECTORS.Mongos
SERVER_TYPE_SELECTORS.RSPrimary
SERVER_TYPE_SELECTORS.RSSecondary
SERVER_TYPE_SELECTORS.RSOther
SERVER_TYPE_SELECTORS.RSGhost
SERVER_TYPE_SELECTORS.Standalone
"""
