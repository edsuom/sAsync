# sAsync:
# An enhancement to the SQLAlchemy package that provides persistent
# item-value stores, arrays, and dictionaries, and an access broker for
# conveniently managing database access, table setup, and
# transactions. Everything can be run in an asynchronous fashion using
# the Twisted framework and its deferred processing capabilities.
#
# Copyright (C) 2006, 2015 by Edwin A. Suominen, http://edsuom.com
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

"""
Unit tests for sasync.database
"""

import re, time
from twisted.python.failure import Failure
from twisted.internet import reactor, defer

import sqlalchemy as SA

from asynqueue import info, iteration

from database import transact, AccessBroker


DELAY = 0.5
#DB_URL = 'mysql://test@localhost/test'
DB_URL = 'sqlite://'


class PeopleBroker(AccessBroker):
    defaultRoster = (
        ("Theodore",    "Roosevelt"), # 1
        ("Franklin",    "Roosevelt"), # 2
        ("Martin",      "Luther"),    # 3
        ("Ronald",      "Reagan"),    # 4
        ("Russ",        "Feingold"))  # 5
    
    def __init__(self, url, **kw):
        self.matches = {}
        AccessBroker.__init__(self, url, **kw)

    def startup(self):
        return self.table(
            'people',
            SA.Column('id', SA.Integer, primary_key=True),
            SA.Column('name_first', SA.String(32)),
            SA.Column('name_last', SA.String(32)))

    @transact
    def first(self):
        self.people.delete().execute()
        for firstName, lastName in self.defaultRoster:
            self.people.insert().execute(
                name_first=firstName, name_last=lastName)
        
    @transact
    def everybody(self):
        rp = SA.select(
            [self.people.c.name_last, self.people.c.name_first]).execute()
        # Iteration-ready; we return the ResultProxy, not a list of
        # rows from rp.fetchall()
        return rp
        
    @transact
    def addPerson(self, firstName, lastName):
        self.people.insert().execute(
            name_last=lastName, name_first=firstName)

    @transact
    def familyMembers(self, lastName):
        # Uses a selex object nested inside a transaction
        with self.selex(self.people.c.name_first) as sh:
            sh.where(self.people.c.name_last == lastName)
        return [x[0] for x in sh().fetchall()]
    
    @transact
    def fakeTransaction(self, x):
        x += 1
        time.sleep(0.2)
        return x

    @transact
    def erroneousTransaction(self):
        time.sleep(0.1)
        raise Exception("Error raised for testing")
        
    @transact
    def nestedTransaction(self, x):
        x += 1
        time.sleep(0.2)
        return self.fakeTransaction(x)

    @transact
    def makeFoobarTable(self):
        return self.table(
            'foobars',
            SA.Column('id', SA.Integer, primary_key=True),
            SA.Column('foobar', SA.String(64)))

