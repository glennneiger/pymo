====
pymo
====

``pymo`` simply exports a factory function (``pymo.mongo_client``) that will enable
"fail fast" semantics on client creation. This was motivated by changes
introduced in ``pymongo>=3`` that will cause calls to block for
``ServerSelectionTimeoutMS`` (see the
[specs](https://github.com/mongodb/specifications/blob/master/source/server-selection/server-selection.rst))
when servers are either down or do not meet the requirements for a particular
operation (e.g., we are trying to insert while there is no primary).

Usage
-----

``pymo`` takes the same arguments as ``pymongo.MongoClient`` and adds three
more keyword arguments:

- ``fail_fast``: a ``bool`` indicating whether you want "fail fast" semantics
  on client creation
- ``state_selectors``: a list of server state selectors (see
  [Server State Selectors](#Server State Selectors))
- ``type_selectors``: a list of server type selectors (see
  [Server Type Selectors](#Server Type Selectors))

Semantics
---------

If ``pymo.mongo_client`` is called with ``fail_fast=True`` (this is the
default), it will create an instance of ``pymongo.MongoClient`` passing the
arguments you passed to ``pymongo.mongo_client`` along with ensuring that
``connect=True``. It will then block, listening for server heartbeat events.
Every time a heartbeat is received, it will update the server state/type for
the seed that that heartbeat corresponds to (*NOTE*: it will ignore heartbeats
for servers that are not in the initial seed list). The call will return when
either:

- there is at least one seed for which we have received a heartbeat that
  matches *any* of the selectors (state or type) passed to
  ``pymo.mongo_client``
- heartbeats have been received for all seeds, no selectors were specified, and
  no errors (e.g., ``ECONNREFUSED``) have been logged

If neither of these conditions are met, an error of type
``pymo.error.ServerSelectionTryOnceTimeoutError`` will be raised.

Server Selectors
----------------

Server Type Selectors
^^^^^^^^^^^^^^^^^^^^^

- ``SERVER_TYPE_SELECTORS.Unknown``
- ``SERVER_TYPE_SELECTORS.Mongos``
- ``SERVER_TYPE_SELECTORS.RSPrimary``
- ``SERVER_TYPE_SELECTORS.RSSecondary``
- ``SERVER_TYPE_SELECTORS.RSOther``
- ``SERVER_TYPE_SELECTORS.RSGhost``
- ``SERVER_TYPE_SELECTORS.Standalone``

Server State Selectors
^^^^^^^^^^^^^^^^^^^^^^

- ``SERVER_STATE_SELECTORS.Writable``
- ``SERVER_STATE_SELECTORS.Readable``

