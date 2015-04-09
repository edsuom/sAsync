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
Unit tests for sasync.parray.py
"""

from twisted.internet import defer

import sqlalchemy as SA

from sasync.database import transact, AccessBroker
import sasync.parray as parray
from testbase import TestCase


GROUP_ID = 123
VERBOSE = False

db = 'parray.db'


class Parray:
    def setUp(self):
        self.a = parray.PersistentArray(GROUP_ID, "sqlite:///%s" % db)

    def tearDown(self):
        def _tearDown():
            sa = self.a.t.sasync_array
            sa.delete(sa.c.group_id == GROUP_ID).execute()
        d = self.a.t.deferToQueue(_tearDown, niceness=19)
        d.addCallback(self.a.shutdown)
        return d

    def loadFromDB(self, x, y, z):
        def _loadFromDB():
            sa = self.a.t.sasync_array
            row = sa.select(
                SA.and_(
                    sa.c.group_id == GROUP_ID,
                    sa.c.x == SA.bindparam('x'),
                    sa.c.y == SA.bindparam('y'),
                    sa.c.z == SA.bindparam('z'))
            ).execute(x=x, y=y, z=z).first()
            return row
        return self.a.t.deferToQueue(_loadFromDB)
    
    def writeToDB(self, x, y, z, value):
        def _writeToDB():
            self.a.t.sasync_array.insert().execute(
                group_id=GROUP_ID,
                x=hash(x), y=hash(y), z=hash(z), value=value)
        
        return self.a.t.deferToQueue(_writeToDB)

    def clearDB(self):
        def _clearDB():
            self.a.t.sasync_array.delete(
                self.a.t.sasync_array.c.group_id == GROUP_ID).execute()
        return self.a.t.deferToQueue(_clearDB)


class TestPersistentArray(Parray, TestCase):
    elements = ((1,2,3,'a'), (2,3,4,'b'), (4,5,6,'c'))

    def writeStuff(self):
        dList = []
        for element in self.elements:
            dList.append(self.writeToDB(*element))
        return defer.DeferredList(dList)

    @defer.inlineCallbacks
    def test_writeAndGet(self):
        yield self.clearDB()
        yield self.writeStuff()
        x = yield self.a.get(1,2,3)
        self.assertEqual(x, 'a')
        x = yield self.a.get(2,3,4)
        self.assertEqual(x, 'b')
        x = yield self.a.get(4,5,6)
        self.assertEqual(x, 'c')

    @defer.inlineCallbacks
    def test_overwriteAndGet(self):
        yield self.clearDB()
        yield self.writeStuff()
        x = yield self.a.get(1,2,3)
        self.assertEqual(x, 'a')
        yield self.a.set(1,2,3, 'foo')
        x = yield self.a.get(1,2,3)
        self.assertEqual(x, 'foo')

    @defer.inlineCallbacks
    def test_deleteAndCheck(self):
        yield self.clearDB()
        yield self.writeStuff()
        yield self.a.delete(1,2,3)
        x = yield self.loadFromDB(1,2,3)
        self.assertEqual(x, None)

    @defer.inlineCallbacks
    def test_clearAndCheck(self):
        yield self.clearDB()
        yield self.writeStuff()
        self.a.clear()
        for element in self.elements:
            result = yield self.loadFromDB(*element[0:3])
            self.assertEqual(
                result, None,
                "Should have empty row, got '{}'".format(result))

    @defer.inlineCallbacks
    def test_setAndGet(self):
        yield self.clearDB()
        yield self.a.set(1,2,3, True)
        x = yield self.a.get(1,2,3)
        self.assertEqual(x, True)

    @defer.inlineCallbacks
    def test_dimensions(self):
        yield self.clearDB()
        yield self.writeStuff()
        x = yield self.a.get(1,2,4)
        self.assertEqual(x, None)
        x = yield self.a.get(1,3,3)
        self.assertEqual(x, None)
        x = yield self.a.get(2,2,3)
        self.assertEqual(x, None)
    
