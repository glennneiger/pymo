from __future__ import absolute_import, division, print_function

from distutils.version import LooseVersion

from pymongo import __version__

if LooseVersion(__version__) >= LooseVersion('3.0'):
    from .pymongo3 import mongo_client
else:
    from .pymongo2 import mongo_client
from .selectors import SERVER_STATE_SELECTORS, SERVER_TYPE_SELECTORS
