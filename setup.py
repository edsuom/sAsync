#!/usr/bin/env python
#
# sAsync:
# An enhancement to the SQLAlchemy package that provides persistent
# item-value stores, arrays, and dictionaries, and an access broker for
# conveniently managing database access, table setup, and
# transactions. Everything can be run in an asynchronous fashion using
# the Twisted framework and its deferred processing capabilities.
#
# Copyright (C) 2006, 2015 by Edwin A. Suominen, http://edsuom.com/sAsync
#
# See edsuom.com for API documentation as well as information about
# Ed's background and other projects, software and otherwise.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the
# License. You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an "AS
# IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language
# governing permissions and limitations under the License.

NAME = "sAsync"


### Imports and support
from setuptools import setup

### Define requirements
required = ['SQLAlchemy>=0.5', 'AsynQueue>=0.9.4']


### Define setup options
kw = {'version':           "0.9.3",
      'license':           "Apache License (2.0)",
      'platforms':         "OS Independent",

      'url':               "http://edsuom.com/{}.html".format(NAME),
      'project_urls':      {
          'GitHub':     "https://github.com/edsuom/{}".format(NAME),
          'API':        "http://edsuom.com/{}/{}.html".format(
              NAME, NAME.lower()),
          },
      'author':            "Edwin A. Suominen",
      'author_email':      "foss@edsuom.com",
      'maintainer':        "Edwin A. Suominen",
      'maintainer_email':  "foss@edsuom.com",
      
      'install_requires':  required,
      'packages':          [
          'sasync', 'sasync.test',
      ],
      'test_suite':        "sasync.test",
}

kw['keywords'] = [
    'SQL', 'SQLAlchemy', 'twisted', 'asynchronous', 'async',
    'persist', 'persistence', 'persistent',
    'database', 'sqlite', 'mysql']


kw['classifiers'] = [
    'Development Status :: 5 - Production/Stable',

    'Intended Audience :: Developers',
    
    'License :: OSI Approved :: Apache Software License',
    'Operating System :: OS Independent',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 2 :: Only',
    'Framework :: Twisted',

    'Topic :: Database',
    'Topic :: Database :: Front-Ends',
    'Topic :: Database :: Database Engines/Servers',
    'Topic :: Software Development :: Libraries :: Python Modules',
]

# You get 77 characters. Use them wisely.
kw['description'] =\
"SQLAlchemy done Asynchronously: Twisted-friendly access to an SQL database."

kw['long_description'] = """
An enhancement to the SQLAlchemy_ package that provides asynchronous,
deferred-result access via the Twisted_ framework and an access broker
that conveniently manages database access, table setup, and
transactions. Included are modules for implementing persistent
dictionaries and three-dimensional arrays.

sAsync lets you use the use the database transaction core of the
SQLAlchemy Python SQL toolkit in an asynchronous fashion. In addition
to an access broker for conveniently managing database access, table
setup, and transactions under the Twisted framework, it provides
persistent item-value stores and arrays.

The sAsync package uses a threaded task queue from the AsynQueue_
package to wrap your SQLAlchemy database access code inside
asynchronous transactions. At the lowest level, it provides a
*@transact* decorator for your database-access methods that makes them
immediately return a Twisted Deferred object.

Python 3 compatiblity is in the works, but not yet supported.

.. _SQLAlchemy: https://www.sqlalchemy.org/

.. _Twisted: https://twistedmatrix.com/trac/

.. _AsynQueue: http://edsuom.com/AsynQueue.html

"""

### Finally, run the setup
setup(name=NAME, **kw)
