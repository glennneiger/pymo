from __future__ import absolute_import, division, print_function

import os
import re
import sys

from setuptools import setup, find_packages


_locals = {}
with open('pymo/_version.py') as fd:
    exec(fd.read(), None, _locals)
version = _locals['__version__']


setup(
    name='pymo',
    version=version,
    description='MongoClient instrumentation and factory',
    url='https://github.com/mlab/pymo',
    author='Greg Banks',
    license='MIT',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7'
    ],
    keywords='pymongo mongodb driver',
    packages=find_packages(exclude=['tests']),
    install_requires=['pymongo>=2,<4'],
    extras_require={
        'test': ['nose2'],
    }
)
