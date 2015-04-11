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
from testbase import MsgBase


VERBOSE = False

DELAY = 0.5
#DB_URL = 'mysql://test@localhost/test'
DB_URL = 'sqlite://'


class PeopleBroker(MsgBase, AccessBroker):
    verbose = False
    
    defaultRoster = (
        ("Theodore",    "Roosevelt"),
        ("Franklin",    "Roosevelt"),
        ("Martin",      "Luther"),
        ("Ronald",      "Reagan"),
        ("Russ",        "Feingold"))
    
    def __init__(self, url, verbose=False, spew=False, returnFailure=None):
        self.matches = {}
        if verbose:
            self.verbose = True
        AccessBroker.__init__(
            self, url,
            verbose=self.isVerbose(),
            spew=spew, returnFailure=returnFailure)

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
    def matchingNames(self, letter):
        def fullName(row):
            firstName = row[self.people.c.name_first].capitalize()
            lastName = row[self.people.c.name_last].capitalize()
            return "%s %s" % (firstName, lastName)
        
        s = self.s
        if not s('letterMatch'):
            s([self.people],
              and_(self.people.c.name_first.like(bindparam('first')),
                  self.people.c.name_last.like(bindparam('last'))))
        match = "%" + letter + "%"
        rows = s().execute(first=match, last=match).fetchall()
        names = [fullName(row) for row in rows]
        print "MN", rows, names
        for name in names:
            self.matches.setdefault(name, [])
            self.matches[name].append(letter)
        
    @transact
    def addPerson(self, firstName, lastName):
        self.people.insert().execute(
            name_last=lastName, name_first=firstName)

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

