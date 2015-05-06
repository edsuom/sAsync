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
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

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
