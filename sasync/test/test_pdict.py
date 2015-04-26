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
Unit tests for sasync.pdict.py
"""

from twisted.internet import defer

import sqlalchemy as SA

from queue import Factory
from pdict import PersistentDict
from parray import PersistentArray

from testbase import deferToDelay, TestCase


ID = 341
VERBOSE = False


db = '/tmp/pdict.db'
URL = "sqlite:///%s" % db
Factory.setGlobal(URL)


def itemsEqual(itemsA, itemsB):
    if VERBOSE:
        print "\n%s\n\tvs\n%s\n" % (str(itemsA), str(itemsB))
    for item in itemsA:
        if not itemsB.count(item):
            print "Item '%s' of list A not in list B" % (item,)
            return False
    for item in itemsB:
        if not itemsA.count(item):
            print "Item '%s' of list B not in list A" % (item,)
            return False
    return True


class TestPlayNice(TestCase):
    def test_shutdown(self):
        def first(null):
            y = PersistentDict('alpha')
            d = y['eagle']
            d.addCallback(self.failUnlessEqual, 'bald')
            return d
        
        x = PersistentDict('alpha')
        x['eagle'] = 'bald'
        return x.shutdown().addCallback(first)

    @defer.inlineCallbacks
    def test_sequentiallyStartedDicts(self):
        x = PersistentDict('alpha')
        y = PersistentDict('bravo')
        yield x.preload()
        yield y.preload()
        x['a'] = 1
        y['a'] = 10
        yield x.deferToWrites()
        self.failUnlessEqual(x['a'], 1)
        yield y.deferToWrites()
        self.failUnlessEqual(y['a'], 10)
        yield x.shutdown()
        yield y.shutdown()

    def test_threeSeparateDicts(self):
        def first():
            self.x['a'] = 1
            self.y['a'] = 10
            self.z['a'] = 100
            return defer.DeferredList([
                self.x.deferToWrites(),
                self.y.deferToWrites(),
                self.z.deferToWrites()])

        def second(null):
            d = self.x['a']
            d.addCallback(self.failUnlessEqual, 1)
            d.addCallback(lambda _: self.y['a'])
            d.addCallback(self.failUnlessEqual, 10)
            d.addCallback(lambda _: self.z['a'])
            d.addCallback(self.failUnlessEqual, 100)
            return d

        def third(null):
            def wait():
                return deferToDelay(1.0)
            
            def thisOneShutdown(null, objectName):
                self.msg("Done shutting down PDict '{}'", objectName)

            dList = []
            for objectName in ('x', 'y', 'z'):
                d1 = getattr(self, objectName).shutdown()
                self.msg("About to shut down Pdict '{}'", objectName)
                d1.addCallback(thisOneShutdown, objectName)
                d2 = wait()
                dList.append(defer.DeferredList([d1,d2]))
            return defer.DeferredList(dList)

        self.x = PersistentDict('alpha')
        self.y = PersistentDict('bravo')
        self.z = PersistentDict('charlie')
        
        d = first()
        d.addCallback(second)
        d.addCallback(third)
        return d

    def test_threeSeparatePreloadedDicts(self):
        def first():
            d1 = self.x.preload()
            d2 = self.y.preload()
            d3 = self.z.preload()
            d4 = deferToDelay(0.5)
            return defer.DeferredList([d1,d2,d3,d4])

        def second(null):
            self.x['a'] = 1
            self.y['a'] = 10
            self.z['a'] = 100
            return defer.DeferredList([
                self.x.deferToWrites(),
                self.y.deferToWrites(),
                self.z.deferToWrites()])

        def third(null):
            self.failUnlessEqual(self.x['a'], 1)
            self.failUnlessEqual(self.y['a'], 10)
            self.failUnlessEqual(self.z['a'], 100)

        def fourth(null):
            def wait():
                return deferToDelay(1.0)
            
            def thisOneShutdown(null, objectName):
                self.msg("Done shutting down PDict '{}'", objectName)

            dList = []
            for objectName in ('x', 'y', 'z'):
                d1 = getattr(self, objectName).shutdown()
                self.msg("About to shut down Pdict '{}'", objectName)
                d1.addCallback(thisOneShutdown, objectName)
                d2 = wait()
                dList.append(defer.DeferredList([d1,d2]))
            return defer.DeferredList(dList)

        self.x = PersistentDict('alpha')
        self.y = PersistentDict('bravo')
        self.z = PersistentDict('charlie')
        
        d = first()
        d.addCallback(second)
        d.addCallback(third)
        d.addCallback(fourth)
        return d

    def test_oneDictWithParray(self):
        import sasync.parray as parray
        
        x = PersistentDict('foo')
        y = PersistentArray('bar')
        
        def first():
            x['a'] = 1
            return x.deferToWrites()
        
        def second(null):
            d = x['a']
            d.addCallback(self.failUnlessEqual, 1)
            return d
        
        def third(null):
            return defer.DeferredList([x.shutdown(), y.shutdown()])
        
        d = first()
        d.addCallback(second)
        d.addCallback(third)
        return d

    def test_twoDictsWithParray(self):
        import sasync.parray as parray
        
        x = PersistentDict('foo')
        y = PersistentDict('bar')
        z = PersistentArray('whatever')
        
        def first():
            x['a'] = 1
            y['a'] = 10
            return defer.DeferredList([x.deferToWrites(), y.deferToWrites()])
        
        def second(null):
            d = x['a']
            d.addCallback(self.failUnlessEqual, 1)
            d.addCallback(lambda _: y['a'])
            d.addCallback(self.failUnlessEqual, 10)
            return d
        
        def third(null):
            return defer.DeferredList([
                x.shutdown(), y.shutdown(), z.shutdown()])
        
        d = first()
        d.addCallback(second)
        d.addCallback(third)
        return d


class Pdict:
    def tearDown(self):
        return self.p.shutdown()
    
    def writeToDB(self, **items):
        def _writeToDB(insertionList):
            if VERBOSE:
                print "\nWRITE-TO-DB", insertionList, "\n\n"
            self.pit.sasync_items.insert().execute(*insertionList)
        
        insertionList = []
        for name, value in items.iteritems():
            insertionList.append({'group_id':ID, 'name':name, 'value':value})
        return self.pit.deferToQueue(_writeToDB, insertionList)

    def clearDB(self):
        def _clearDB():
            if VERBOSE:
                print "\nCLEAR-DB\n"
            self.pit.sasync_items.delete(
                self.pit.sasync_items.c.group_id == ID).execute()
        
        return self.pit.deferToQueue(_clearDB)


class PdictNormal(Pdict):
    def setUp(self):
        def started(null):
            self.pit = self.p.i.t
            self.p.data.clear()
            return self.pit.deferToQueue(clear)
        
        def clear():
            si = self.pit.sasync_items
            si.delete(si.c.group_id == ID).execute()

        self.p = PersistentDict(ID)
        d = self.p.i.t.startup()
        d.addCallback(started)
        return d


class TestPdictNormalCore(PdictNormal, TestCase):
    def test_someWriteSomeSet(self):
        def setStuff(null):
            self.p['b'] = 'beta'
            self.p['d'] = 'delta'
            d = self.p.deferToWrites()
            d.addCallback(lambda _: self.p.items())
            d.addCallback(
                itemsEqual,
                [('a','alpha'),
                 ('b','beta'),
                 ('c','charlie'),
                 ('d','delta')])
            d.addCallback(self.failUnless, "Items not equal")
            return d
        return self.writeToDB(
            a='alpha', b='bravo', c='charlie').addCallback(setStuff)

    @defer.inlineCallbacks
    def test_writeAndGet(self):
        yield self.writeToDB(a=100, b=200, c='foo')
        x = yield self.p['a']
        self.failUnlessEqual(x, 100)
        y = yield self.p['b']
        self.failUnlessEqual(y, 200)
        z = yield self.p['c']
        self.failUnlessEqual(z, 'foo')


class TestPdictNormalMain(PdictNormal, TestCase):
    def test_loadAll(self):
        def checkItems(items):
            self.failUnlessEqual(items, {'a':1, 'b':2})
            
        d = self.writeToDB(a=1, b=2)
        d.addCallback(lambda _: self.p.loadAll())
        d.addCallback(checkItems)
        return d

    @defer.inlineCallbacks
    def test_setAndGet(self):
        self.p['a'] = 10        
        yield self.p.deferToWrites()
        value = yield self.p['a']
        self.failUnlessEqual(value, 10)

    def test_setAndLoadAll(self):
        self.p['a'] = 1
        self.p['b'] = 2

        d = self.p.deferToWrites()
        d.addCallback(lambda _: self.p.loadAll())
        d.addCallback(self.failUnlessEqual, {'a':1, 'b':2})
        return d
            
    def test_setdefaultEmpty(self):
        self.p['a'] = 1
        self.p.writeTracker.put(self.p.setdefault('b', 2))

        d = self.p.deferToWrites()
        d.addCallback(lambda _: self.p.loadAll())
        d.addCallback(self.failUnlessEqual, {'a':1, 'b':2})
        return d

    def test_setClearAndLoadAll(self):
        self.p['a'] = 1
        self.p['b'] = 2
        self.p.clear()
        d = self.p.deferToWrites()
        d.addCallback(lambda _: self.p.loadAll())
        d.addCallback(self.failUnlessEqual, {})
        return d

    def test_setdefaultSet(self):
        self.p['a'] = 1
        self.p.writeTracker.put(self.p.setdefault('a', 2))

        d = self.p.deferToWrites()
        d.addCallback(lambda _: self.p.items())
        d.addCallback(self.failUnlessEqual, [('a',1)])
        return d

    def test_setAndGetComplex(self):
        self.p['a'] = 1
        self.p['b'] = 2
        self.p.writeTracker.put(self.p.setdefault('b', 20))
        self.p.writeTracker.put(self.p.setdefault('c', 3))
        self.p.update({'d':4, 'e':5})

        d = self.p.deferToWrites()
        d.addCallback(lambda _: self.p.items())
        d.addCallback(
            itemsEqual, [('a',1), ('b',2), ('c',3), ('d',4), ('e',5)])
        d.addCallback(self.failUnless, "Items not equal")
        return d

    def test_keys(self):
        d = self.writeToDB(a=1.1, b=1.2, c=1.3)
        d.addCallback(lambda _: self.p.keys())
        d.addCallback(itemsEqual, ['a', 'b', 'c'])
        d.addCallback(self.failUnless)
        return d

    def test_values(self):
        d = self.writeToDB(a=1.1, b=1.2, c=1.3)
        d.addCallback(lambda _: self.p.values())
        d.addCallback(itemsEqual, [1.1, 1.2, 1.3])
        d.addCallback(self.failUnless)
        return d

    def test_items(self):
        d = self.writeToDB(a=2.1, b=2.2, c=2.3)
        d.addCallback(lambda _: self.p.items())
        d.addCallback(itemsEqual, [('a',2.1), ('b',2.2), ('c',2.3)])
        d.addCallback(self.failUnless)
        return d

    def test_contains(self):
        def one(null):
            d = self.p.has_key('a')
            d.addCallback(
                self.failUnless, "Item 'a' should be in the dictionary")
            return d
        
        def another(null):
            d = self.p.has_key('d')
            d.addCallback(
                self.failIf, "Item 'd' shouldn't be in the dictionary")
            return d
        
        d = self.clearDB()
        d.addCallback(lambda _: self.writeToDB(a=1.1, b=1.2, c=1.3))
        d.addCallback(one)
        d.addCallback(another)
        return d

    def test_getMethod(self):
        d = self.clearDB()
        d.addCallback(lambda _: self.writeToDB(a='present'))
        d.addCallback(lambda _: self.p.get('a', None))
        d.addCallback(self.failUnlessEqual, 'present')
        d.addCallback(lambda _: self.p.get('b', None))
        d.addCallback(self.failUnlessEqual, None)
        return d


class PdictPreload(Pdict):
    def setUp(self):
        def started(null):
            self.pit = self.p.i.t
            self.p.data.clear()
            return self.pit.deferToQueue(clear)
        
        def clear():
            si = self.pit.sasync_items
            si.delete(si.c.group_id == ID).execute()

        self.p = PersistentDict(ID)
        d = self.p.preload()
        d.addCallback(started)
        return d


class TestPdictPreload(PdictPreload, TestCase):
    def test_setAndGet(self):
        self.p['a'] = 10        
        self.failUnlessEqual(self.p['a'], 10)

    def test_setActuallyWrites(self):
        def first(null):
            self.p['a'] = 'new'
            return self.p.deferToWrites()
        def second(null):
            def _second():
                si = self.pit.sasync_items
                row = si.select(
                    SA.and_(
                    si.c.group_id==ID, si.c.name=='a')).execute().first()
                return row['value']
            return self.pit.deferToQueue(_second)
        d = self.clearDB()
        d.addCallback(first)
        d.addCallback(second)
        d.addCallback(self.failUnlessEqual, 'new')
        return d
