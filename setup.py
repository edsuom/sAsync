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
required = ['SQLAlchemy>=0.5', 'AsynQueue>=0.8.1']


### Define setup options
kw = {'version':'0.8.2',
      'license':'GPL',
      'platforms':'OS Independent',

      'url':"http://edsuom.com/{}.html".format(NAME),
      'author':'Edwin A. Suominen',
      'author_email':"valueprivacy-foss@yahoo.com",
      'maintainer':'Edwin A. Suominen',
      
      'install_requires':required,
      'packages':['sasync'],
      
      'zip_safe':True
      }

kw['keywords'] = [
    'SQL', 'SQLAlchemy', 'Twisted', 'asynchronous',
    'persist', 'persistence', 'persistent',
    'database', 'graph']

kw['classifiers'] = [
    'Development Status :: 5 - Production/Stable',
    
    'Intended Audience :: Developers',

    'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
    'Operating System :: OS Independent',
    'Framework :: Twisted',

    'Programming Language :: Python',
    'Programming Language :: SQL',

    'Topic :: Database',
    'Topic :: Database :: Front-Ends',
    'Topic :: Software Development :: Libraries :: Python Modules',
    ]

kw['description'] = " ".join("""
SQLAlchemy done Asynchronously, with a convenient transacting database access
broker and persistent dictionaries, arrays, and graphs.
""".split("\n"))

kw['long_description'] = " ".join("""
An enhancement to the SQLAlchemy package that provides asynchronous,
deferred-result access via the Twisted framework and an access broker that
conveniently managing database access, table setup, and transactions. Included
are modules for implementing persistent dictionaries, three-dimensional arrays,
and graph objects.
""".split("\n"))

### Finally, run the setup
setup(name=NAME, **kw)
