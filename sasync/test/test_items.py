# sAsync:
# An enhancement to the SQLAlchemy package that provides persistent
# item-value stores, arrays, and dictionaries, and an access broker for
# conveniently managing database access, table setup, and
# transactions. Everything can be run in an asynchronous fashion using
# the Twisted framework and its deferred processing capabilities.
#
# Copyright (C) 2006, 2015 by Edwin A. Suominen, http://edsuom.com
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

"""
Unit tests for sasync.items.py.
"""

from twisted.internet.defer import Deferred, DeferredList

from sqlalchemy import *

from sasync.database import transact, AccessBroker
import sasync.items as items
from testbase import MockThing, TestCase


GROUP_ID = 123
VERBOSE = False

db = 'items.db'
    

class TestableItemsTransactor(items.Transactor):
    @transact
    def pre(self):
        # Group 123
        self.sasync_items.insert().execute(
            group_id=123, name='foo', value='OK')
        # Set up an experienced MockThing to have pickled
        thing = MockThing()
        thing.method(1)
        self.sasync_items.insert().execute(
            group_id=123, name='bar', value=thing)
        # Group 124
        self.sasync_items.insert().execute(
            group_id=124, name='foo', value='bogus')
        self.sasync_items.insert().execute(
            group_id=124, name='invalid', value='bogus')

    @transact
    def post(self):
        self.sasync_items.delete().execute()


class ItemsMixin:
    def tearDown(self):
        def _tearDown():
            si = self.i.t.sasync_items
            si.delete(si.c.group_id == GROUP_ID).execute()
        d = self.i.t.deferToQueue(_tearDown, niceness=20)
        d.addCallback(lambda _: self.i.shutdown())
        return d


class TestItemsTransactor(ItemsMixin, TestCase):
    def setUp(self):
        url = "sqlite:///%s" % db
        self.i = items.Items(GROUP_ID, url)
        self.i.t = TestableItemsTransactor(self.i.groupID, url)
        return self.i.t.pre()

    def tearDown(self):
        return self.i.t.post()

    def test_load(self):
        def gotValue(value, name):
            if name == 'foo':
                self.failUnlessEqual(value, 'OK')
            else:
                self.failUnless(
                    isinstance(value, MockThing),
                    "Item 'bar' is a '%s', not an instance of 'MockThing'" \
                    % value)
                self.failUnless(
                    value.beenThereDoneThat,
                    "Class instance wasn't properly persisted with its state")
                self.failUnlessEqual(
                    value.method(2.5), 5.0,
                    "Class instance wasn't properly persisted with its method")

        dList = []
        for name in ('foo', 'bar'):
            dList.append(self.i.t.load(name).addCallback(gotValue, name))
        return DeferredList(dList)

    def test_load(self):
        def gotValue(value, name):
            if name == 'foo':
                self.failUnlessEqual(value, 'OK')
            else:
                self.failUnless(
                    isinstance(value, MockThing),
                    "Item 'bar' is a '%s', not an instance of 'MockThing'" \
                    % value)
                self.failUnless(
                    value.beenThereDoneThat,
                    "Class instance wasn't properly persisted with its state")
                self.failUnlessEqual(
                    value.method(2.5), 5.0,
                    "Class instance wasn't properly persisted with its method")

        dList = []
        for name in ('foo', 'bar'):
            dList.append(self.i.t.load(name).addCallback(gotValue, name))
        return DeferredList(dList)

    def test_loadAbsent(self):
        def gotValue(value):
            self.failUnless(
                isinstance(value, items.Missing),
                "Should have returned 'Missing' object, not '%s'!" % \
                str(value))
        def gotExpectedError(failure):
            self.fail("Shouldn't have raised error on missing value")
        return self.i.t.load('invalid').addCallbacks(
            gotValue, gotExpectedError)

    def test_loadAll(self):
        def loaded(items):
            itemKeys = items.keys()
            itemKeys.sort()
            self.failUnlessEqual(itemKeys, ['bar', 'foo'])
        return self.i.t.loadAll().addCallback(loaded)

    def insertLots(self, callback):
        noviceThing = MockThing()
        experiencedThing = MockThing()
        experiencedThing.method(0)
        self.whatToInsert = {
            'alpha':5937341,
            'bravo':'abc',
            'charlie':-3.1415,
            'delta':(1,2,3),
            'echo':True,
            'foxtrot':False,
            'golf':noviceThing,
            'hotel':experiencedThing,
            'india':MockThing
            }
        dList = []
        for name, value in self.whatToInsert.iteritems():
            dList.append(self.i.t.insert(name, value))
        return DeferredList(dList).addCallback(
            callback, self.whatToInsert.copy())

    def test_insert(self):
        def done(null, items):
            def check():
                table = self.i.t.sasync_items
                for name, inserted in items.iteritems():
                    value = table.select(
                        and_(table.c.group_id == 123,
                             table.c.name == name)
                        ).execute().fetchone()['value']
                    msg = "Inserted '{}:{}' ".format(name, inserted) +\
                          "but read '{}' back from the database!".format(value)
                    self.failUnlessEqual(value, inserted, msg)
                    for otherName, otherValue in items.iteritems():
                        if otherName != name and value == otherValue:
                            self.fail(
                                "Inserted item '%s' is equal to item '%s'" % \
                                (name, otherName))
            return self.i.t.deferToQueue(check)
        return self.insertLots(done)

    def test_deleteOne(self):
        def gotOriginal(value):
            self.failUnlessEqual(value, 'OK')
            return self.i.t.delete('foo').addCallback(getAfterDeleted)
        def getAfterDeleted(null):
            return self.i.t.load('foo').addCallback(checkIfDeleted)
        def checkIfDeleted(value):
            self.failUnless(isinstance(value, items.Missing))
        return self.i.t.load('foo').addCallback(gotOriginal)

    def test_deleteMultiple(self):
        def getAfterDeleted(null):
            return self.i.t.loadAll().addCallback(checkIfDeleted)
        def checkIfDeleted(values):
            self.failUnlessEqual(values, {})
        return self.i.t.delete('foo', 'bar').addCallback(getAfterDeleted)

    def test_namesFew(self):
        def got(names):
            names.sort()
            self.failUnlessEqual(names, ['bar', 'foo'])
        return self.i.t.names().addCallback(got)

    def test_namesMany(self):
        def get(null, items):
            return self.i.t.names().addCallback(got, items.keys())
        def got(names, shouldHave):
            shouldHave += ['foo', 'bar']
            names.sort()
            shouldHave.sort()
            self.failUnlessEqual(names, shouldHave)
        return self.insertLots(get)

    def test_update(self):
        def update(null, items):
            return DeferredList([
                self.i.t.update('alpha',   1),
                self.i.t.update('bravo',   2),
                self.i.t.update('charlie', 3)
                ]).addCallback(check, items)
        def check(null, items):
            return self.i.t.loadAll().addCallback(loaded, items)
        def loaded(loadedItems, controlItems):
            controlItems.update({'alpha':1, 'bravo':2, 'charlie':3})
            for name, value in controlItems.iteritems():
                self.failUnlessEqual(
                    value, loadedItems.get(name, 'Impossible Value'))
        return self.insertLots(update)

    
class TestItems(ItemsMixin, TestCase):
    def setUp(self):
        self.i = items.Items(GROUP_ID, "sqlite:///%s" % db)
    
    def test_insertAndLoad(self):
        nouns = ('lamp', 'rug', 'chair')
        def first(null):
            return self.i.loadAll().addCallback(second)
        def second(items):
            self.failUnlessEqual(items['Nouns'], nouns)
        return self.i.insert('Nouns', nouns).addCallback(first)

    def test_insertAndDelete(self):
        items = {'a':0, 'b':1, 'c':2, 'd':3, 'e':4}

        def first(null):
            return self.i.delete('c').addCallback(second)

        def second(null):
            return self.i.names().addCallback(third)

        def third(nameList):
            desiredList = [x for x in items.keys() if x != 'c']
            desiredList.sort()
            nameList.sort()
            self.failUnlessEqual(nameList, desiredList)

        dL = []
        for name, value in items.iteritems():
            dL.append(self.i.insert(name, value))
        return DeferredList(dL).addCallback(first)

    def test_insertAndLoadAll(self):
        items = {'a':0, 'b':1, 'c':2, 'd':3, 'e':4}
        def first(null):
            return self.i.loadAll().addCallback(second)
        def second(loadedItems):
            self.failUnlessEqual(loadedItems, items)

        dL = []
        for name, value in items.iteritems():
            dL.append(self.i.insert(name, value))
        return DeferredList(dL).addCallback(first)

    def test_insertAndUpdate(self):
        items = {'a':0, 'b':1, 'c':2, 'd':3, 'e':4}
        def first(null):
            return self.i.update('b', 10).addCallback(second)
        def second(null):
            return self.i.loadAll().addCallback(third)
        def third(loadedItems):
            expectedItems = {'a':0, 'b':10, 'c':2, 'd':3, 'e':4}
            self.failUnlessEqual(loadedItems, expectedItems)

        dL = []
        for name, value in items.iteritems():
            dL.append(self.i.insert(name, value))
        return DeferredList(dL).addCallback(first)


class TestItemsIntegerNames(ItemsMixin, TestCase):
    def setUp(self):
        self.items = {'1':'a', 2:'b', 3:'c', '04':'d'}
        self.i = items.Items(GROUP_ID, "sqlite:///%s" % db, nameType=int)

    def insertStuff(self):
        dL = []
        for name, value in self.items.iteritems():
            dL.append(self.i.insert(name, value))
        return DeferredList(dL)

    def test_names(self):
        def first(null):
            return self.i.names().addCallback(second)
        def second(names):
            names.sort()
            self.failUnlessEqual(names, [1, 2, 3, 4])
        return self.insertStuff().addCallback(first)

    def test_loadAll(self):
        def first(null):
            return self.i.loadAll().addCallback(second)
        def second(loaded):
            self.failUnlessEqual(loaded, {1:'a', 2:'b', 3:'c', 4:'d'})
        return self.insertStuff().addCallback(first)


class TestItemsStringNames(ItemsMixin, TestCase):
    def setUp(self):
        self.items = {'1':'a', 2:'b', u'3':'c', "4":'d'}
        self.i = items.Items(GROUP_ID, "sqlite:///%s" % db, nameType=str)

    def insertStuff(self):
        dL = []
        for name, value in self.items.iteritems():
            dL.append(self.i.insert(name, value))
        return DeferredList(dL)

    def test_names(self):
        def first(null):
            return self.i.names().addCallback(second)
        def second(names):
            names.sort()
            self.failUnlessEqual(names, ['1', '2', '3', '4'])
        return self.insertStuff().addCallback(first)        

    def test_loadAll(self):
        def first(null):
            return self.i.loadAll().addCallback(second)
        def second(loaded):
            self.failUnlessEqual(loaded, {'1':'a', '2':'b', '3':'c', '4':'d'})
        return self.insertStuff().addCallback(first)
        

