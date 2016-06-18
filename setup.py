import os
import re
import sys

from setuptools import setup, find_packages


prefix = os.path.abspath(os.path.dirname(__file__))
version = re.search(r'^__version__\s*=\s*([\'"].+[\'"]).*',
                    open(os.path.join(prefix,
                                      'pymo',
                                      '_version.py')).read()).group(1)


setup(
    name='pymo',
    version=version,
    description='MongoClient instrumentation and factory',
    url='https://github.com/mlab/pymo',
    author='Greg Banks',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: pymongo',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    keywords='pymongo mongodb driver',
    packages=find_packages(exclude=['tests']),
    install_requires=['pymongo>=3.2'],
    extras_require={
        'test': ['nose2'],
    }
)
