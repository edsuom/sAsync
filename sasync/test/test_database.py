# sAsync:
# An enhancement to the SQLAlchemy package that provides persistent
# dictionaries, text indexing and searching, and an access broker for
# conveniently managing database access, table setup, and
# transactions. Everything can be run in an asynchronous fashion using the
# Twisted framework and its deferred processing capabilities.
#
# Copyright (C) 2006 by Edwin A. Suominen, http://www.eepatents.com
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation; either version 2 of the License, or (at your option) any later
# version.
# 
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the file COPYING for more details.
# 
# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc., 51
# Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA

"""
Unit tests for sasync.database
"""

import re, time
from twisted.python.failure import Failure
from twisted.internet import reactor, defer

from sqlalchemy import *

from database import AccessBroker, transact
from testbase import deferToDelay, IterationConsumer, TestCase


VERBOSE = False

DELAY = 0.5
DB_URL = 'sqlite:///database.db'


class MyBroker(AccessBroker):
    def __init__(self, url):
        AccessBroker.__init__(self, url)
        self.matches = {}

    def oops(self, failure):
        print "\FAILURE (MyBroker):", 
        failure.printTraceback()
        print "\n"
        return failure

    def fullName(self, row):
        firstName = row[self.people.c.name_first].capitalize()
        lastName = row[self.people.c.name_last].capitalize()
        return "%s %s" % (firstName, lastName)

    def showNames(self, *args):
        if VERBOSE:
            msgProto = "%20s : %s"
            print ("\n\n" + msgProto) % \
                  ("FULL NAME", "LETTERS IN BOTH FIRST & LAST NAME")
            print '=' * 60
            for name, matchList in self.matches.iteritems():
                print msgProto % (name, ",".join(matchList))

    def setUpPeopleTable(self):
        d = self.table(
            'people',
            Column('id', Integer, primary_key=True),
            Column('name_first', String(32)),
            Column('name_last', String(32)))
        d.addCallbacks(self.insertions, self.oops)
        return d

    @transact
    def tableDelete(self, name):
        table = getattr(self, name)
        #table.delete().execute()

    @transact
    def insertions(self, null):
        self.people.insert().execute(
            name_first='Theodore', name_last='Roosevelt')
        self.people.insert().execute(
            name_first='Franklin', name_last='Roosevelt')
        self.people.insert().execute(
            name_first='Martin', name_last='Luther')
        self.people.insert().execute(
            name_first='Ronald', name_last='Reagan')
        self.people.insert().execute(
            name_first='Russ', name_last='Feingold')

    @transact
    def matchingNames(self, letter):
        s = self.s
        if not s('letterMatch'):
            s([self.people],
              and_(self.people.c.name_first.like(bindparam('first')),
                  self.people.c.name_last.like(bindparam('last'))))
        match = "%" + letter + "%"
        names = [self.fullName(row)
                 for row in s().execute(first=match, last=match).fetchall()]
        for name in names:
            self.matches.setdefault(name, [])
            self.matches[name].append(letter)

    @transact
    def findLastName(self, lastName):
        s = self.s
        if not s('lastName'):
            s([self.people],
              self.people.c.name_last == bindparam('last'))
        return self.fullName(s().execute(last=lastName).fetchone())

    @transact
    def everybody(self):
        rp = select(
            [self.people.c.name_last, self.people.c.name_first]).execute()
        return rp
    
    def addAndUseEntry(self):
        def _first():
            self.people.insert().execute(
                name_last='McCain', name_first='John')

        def _getNewID(null):
            rows = select(
                [self.people.c.id],
                self.people.c.name_last == 'McCain').execute().fetchone()
            return rows[0]

        def _getFirstName(ID):
            return self.people.select().execute(id=ID).fetchone()

        d = transact(_first)()
        d.addCallback(transact, _getNewID)
        d.addCallback(transact, _getFirstName)
        return d

    @transact
    def nestedTransaction(self, x):
        x += 1
        time.sleep(0.2)
        return self.fakeTransaction(x)

    @transact
    def fakeTransaction(self, x):
        x += 1
        time.sleep(0.2)
        return x

    @transact
    def erroneousTransaction(self):
        time.sleep(0.1)
        raise Exception("Error raised for testing")


class AutoSetupBroker(AccessBroker):
    def startup(self):
        d = self.table(
            'people',
            Column('id', Integer, primary_key=True),
            Column('name_first', String(32)),
            Column('name_last', String(32)))
        return d

    def first(self):
        self.people.insert().execute(
            name_first='Firster', name_last='Firstman')

    @transact
    def transactionRequiringFirst(self):
        row = self.people.select().execute(name_first='Firster').fetchone()
        return row['name_last']



class TestStartupAndShutdown(TestCase):
    def setUp(self):
        self.broker = AccessBroker(DB_URL)
        return self.broker.startup()

    @defer.inlineCallbacks
    def test_multipleShutdowns(self):
        for k in xrange(10):
            yield self.broker.shutdown()
            d = defer.Deferred()
            reactor.callLater(0.02, d.callback, None)
            yield d

    def test_shutdownTwoBrokers(self):
        brokerB = AccessBroker(DB_URL)

        def thisOneShutdown(null, broker):
            print "Done shutting down broker '%s'" % broker

        def shutEmDown(null):
            dList = []
            for broker in (self.broker, brokerB):
                d = broker.shutdown()
                if VERBOSE:
                    d.addCallback(thisOneShutdown, broker)
                dList.append(d)
            return defer.DeferredList(dList)

        d = brokerB.startup()
        d.addCallback(shutEmDown)
        return d

    def test_shutdownThreeBrokers(self):
        brokerB = AccessBroker(DB_URL)
        brokerC = AccessBroker(DB_URL)

        def thisOneShutdown(null, broker):
            print "Done shutting down broker '%s'" % broker

        def shutEmDown(null):
            dList = []
            for broker in (self.broker, brokerB, brokerC):
                d = broker.shutdown()
                if VERBOSE:
                    d.addCallback(thisOneShutdown, broker)
                dList.append(d)
            return defer.DeferredList(dList)

        d = defer.DeferredList([brokerB.startup(), brokerC.startup()])
        d.addCallback(shutEmDown)
        return d


class TestPrimitives(TestCase):
    def setUp(self):
        self.broker = MyBroker(DB_URL)

    def tearDown(self):
        return self.broker.shutdown()

    def test_errbackDFQ(self):
        def errback(failure):
            self.failUnless(isinstance(failure, Failure))

        d = self.broker.deferToQueue(lambda x: 1/0, 0)
        d.addCallbacks(
            lambda _: self.fail("Should have done the errback instead"),
            errback)
        return d

    def test_errbackTransact(self):
        def errback(failure):
            self.failUnless(isinstance(failure, Failure))

        d = self.broker.erroneousTransaction()
        d.addCallbacks(
            lambda _: self.fail("Should have done the errback instead"),
            errback)
        return d

    def test_connect(self):
        mutable = []
        def gotConnection(conn):
            mutable.append(conn)

        def gotAll(null):
            prevItem = mutable.pop()
            while mutable:
                thisItem = mutable.pop()
                self.failUnlessEqual(thisItem, prevItem)
                prevItem = thisItem
            
        d1 = self.broker.connect().addCallback(gotConnection)
        d2 = self.broker.connect().addCallback(gotConnection)
        d3 = deferToDelay(DELAY)
        d3.addCallback(lambda _: self.broker.connect())
        d3.addCallback(gotConnection)
        return defer.DeferredList([d1, d2, d3]).addCallback(gotAll)

    def test_connectShutdownConnectAgain(self):
        def newBroker(null):
            self.broker = MyBroker(DB_URL)
            return self.broker.connect()
            
        d = self.broker.connect()
        d.addCallback(lambda _: self.broker.shutdown())
        d.addCallback(newBroker)
        return d

    def test_table(self):
        mutable = []

        def getTable():
            return self.broker.table(
                'very_cool_table',
                Column('id', Integer, primary_key=True),
                Column('Whatever', String(32)))

        def gotTable(table):
            mutable.append(table)

        def gotAll(null):
            self.failUnlessEqual(len(mutable), 3)
            
        d1 = getTable().addCallback(gotTable)
        d2 = getTable().addCallback(gotTable)
        d3 = deferToDelay(DELAY)
        d3.addCallback(lambda _: getTable())
        d3.addCallback(gotTable)
        return defer.DeferredList([d1, d2, d3]).addCallback(gotAll)

    def test_createTableTwice(self):
        def create():
            return self.broker.table(
                'singleton',
                Column('id', Integer, primary_key=True),
                Column('foobar', String(32)))
        return defer.DeferredList([create(), create()])

    def test_createTableWithIndex(self):
        def create():
            return self.broker.table(
                'table_indexed',
                Column('id', Integer, primary_key=True),
                Column('foo', String(32)),
                Column('bar', String(64)),
                index_foobar=['foo', 'bar']
                )
        d = create()
        d.addCallback(lambda _: self.broker.tableDelete('table_indexed'))
        return d

    def test_createTableWithUnique(self):
        def create():
            return self.broker.table(
                'table_unique',
                Column('id', Integer, primary_key=True),
                Column('foo', String(32)),
                Column('bar', String(64)),
                unique_foobar=['foo', 'bar']
                )
        d = create()
        d.addCallback(lambda _: self.broker.tableDelete('table_unique'))
        return d

    def test_sameUrlSameQueueNotStarted(self):
        anotherBroker = MyBroker(DB_URL)
        self.failUnlessEqual(self.broker.q, anotherBroker.q)
        d1 = anotherBroker.shutdown()
        d2 = self.broker.shutdown()
        return defer.DeferredList([d1,d2])

    def test_sameUrlSameQueueStarted(self):
        def doRest(null):
            anotherBroker = MyBroker(DB_URL)
            self.failUnlessEqual(self.broker.q, anotherBroker.q)
            d1 = anotherBroker.shutdown()
            d2 = self.broker.shutdown()
            return defer.DeferredList([d1,d2])
        
        d = defer.Deferred()
        d.addCallback(doRest)
        reactor.callLater(DELAY, d.callback, None)
        return d
        

class TestTransactions(TestCase):
    verbose = True
    
    se = re.compile(r"sqlalchemy.+[eE]ngine")
    st = re.compile(r"sqlalchemy.+[tT]able")
        
    def setUp(self):
        self.broker = MyBroker(DB_URL)

    def tearDown(self):
        return self.broker.shutdown()

    def oops(self, failure):
        self.msg("FAILURE: {}", failure.getTraceback())
        return failure

    def createStuff(self):
        def next(null):
            d = self.broker.table(
                'foobars',
                Column('id', Integer, primary_key=True),
                Column('foobar', String(64)))
            return d
        return self.broker.setUpPeopleTable().addCallbacks(next, self.oops)

    def test_getTable(self):
        def run(null):
            table = self.broker.foobars
            isType = str(type(table))
            self.failUnless(
                self.st.search(isType),
                ("AccessBroker().foobars should be an sqlalchemy Table() "+\
                 "object, but is '%s'") % isType)
        return self.createStuff().addCallbacks(run, self.oops)

    def test_selectOneAndTwoArgs(self):
        s = self.broker.s
        def run(null):
            def next():
                self.failUnlessEqual(s('thisSelect'), False)
                s([self.broker.people], self.broker.people.c.id==1)
                self.failUnlessEqual(s('thisSelect'), True)
                self.failUnlessEqual(s('thatSelect'), False)
            return self.broker.q.call(next)
        return self.createStuff().addCallbacks(run, self.oops)

    def test_selectZeroArgs(self):
        s = self.broker.s
        def run(null):
            def next():
                s('roosevelts')
                s([self.broker.people],
                  self.broker.people.c.name_last == 'Roosevelt')
                rows = s().execute().fetchall()
                return rows
            return self.broker.q.call(next)

        def gotRows(rows):
            nameList = [row[self.broker.people.c.name_first] for row in rows]
            for lastName in ('Franklin', 'Theodore'):
                self.failUnless(lastName in nameList)

        d = self.createStuff()
        d.addCallbacks(run, self.oops)
        d.addCallback(gotRows)
        return d

    @defer.inlineCallbacks
    def test_iterate_transaction(self):
        consumer = IterationConsumer(self.verbose)
        yield self.broker.setUpPeopleTable()
        yield self.broker.everybody(consumer=consumer)
        self.assertEqual(len(consumer.data), 5)
        
    def test_selex_select(self):
        def run(null):
            def next():
                cols = self.broker.people.c
                with self.broker.selex(cols.name_first) as sh:
                    sh.where(cols.name_last == 'Luther')
                    row = sh().fetchone()
                self.assertEqual(row[0], 'Martin')
            return self.broker.q.call(next)
        return self.createStuff().addCallbacks(run, self.oops)

    def test_selex_delete(self):
        def run(null):
            def next():
                table = self.broker.people
                with self.broker.selex(table.delete) as sh:
                    sh.where(table.c.name_last == 'Luther')
                    N = sh().rowcount
                self.assertGreater(N, 0)
            return self.broker.q.call(next)
        return self.createStuff().addCallbacks(run, self.oops)
    
    def test_transactMany(self):
        def run(null):
            dL = []
            for letter in "abcdefghijklmnopqrstuvwxyz":
                d = self.broker.matchingNames(letter)
                dL.append(d)
            return defer.DeferredList(dL).addCallback(self.broker.showNames)
        return self.createStuff().addCallbacks(run, self.oops)

    def test_transactionAutoStartup(self):
        d = self.broker.fakeTransaction(1)
        d.addCallback(self.failUnlessEqual, 2)
        return d

    def test_firstTransaction(self):
        broker = AutoSetupBroker(DB_URL)
        d = broker.transactionRequiringFirst()
        d.addCallback(self.failUnlessEqual, 'Firstman')
        return d

    def test_nestTransactions(self):
        d = self.broker.nestedTransaction(1)
        d.addCallback(self.failUnlessEqual, 3)
        return d
        
        
