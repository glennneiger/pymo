from __future__ import absolute_import, division, print_function

from pymongo.errors import ServerSelectionTimeoutError


class ServerSelectionTryOnceTimeoutError(ServerSelectionTimeoutError):
    """ Fail fast server selection error
    """
    pass

