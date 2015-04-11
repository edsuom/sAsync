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

import logging
from twisted.internet import defer

import sqlalchemy as SA

from asynqueue import info, iteration

from people import PeopleBroker
from testbase import deferToDelay, IterationConsumer, TestCase

from database import transact


VERBOSE = False

DELAY = 0.5
#DB_URL = 'mysql://test@localhost/test'
DB_URL = 'sqlite://'


class TestHandler(logging.Handler):
    def __init__(self, verbose):
        logging.Handler.__init__(self)
        self.verbose = verbose
        self.records = []
        
    def emit(self, record):
        self.records.append(record)
        if self.verbose:
            print "LOGGED:", record.getMessage()


class BrokerTestCase(TestCase):
    spew = False
    
    @defer.inlineCallbacks
    def setUp(self):
        verbose = self.isVerbose()
        if verbose:
            self.handler = TestHandler(True)
            logging.getLogger('asynqueue').addHandler(self.handler)
        self.broker = PeopleBroker(DB_URL, verbose=verbose, spew=self.spew)
        yield self.broker.waitUntilRunning()
        
    @defer.inlineCallbacks
    def tearDown(self):
        if getattr(getattr(self, 'broker', None), 'running', False):
            for tableName in ('people', 'foobars'):
                if hasattr(self.broker, tableName):
                    yield self.broker.sql("DROP TABLE {}".format(tableName))
            yield self.broker.shutdown()

            
class TestBasics(BrokerTestCase):
    verbose = True

    def _oneShutdown(self, null, broker):
        self.msg("Done shutting down broker '{}'",  broker)

    def test_barebones(self):
        self.assertTrue(hasattr(self, 'broker'))
        self.assertTrue(hasattr(self.broker, 'people'))
        
    @defer.inlineCallbacks
    def test_multipleShutdowns(self):
        for k in xrange(10):
            yield self.broker.shutdown()
            yield deferToDelay(0.02)
            
    def test_shutdownTwoBrokers(self):
        brokerB = PeopleBroker(DB_URL)

        def shutEmDown(null):
            dList = []
            for broker in (self.broker, brokerB):
                dList.append(
                    broker.shutdown().addCallback(
                        self._oneShutdown, broker))
            return defer.DeferredList(dList)

        d = brokerB.startup()
        d.addCallback(shutEmDown)
        return d

    def test_shutdownThreeBrokers(self):
        brokerB = PeopleBroker(DB_URL)
        brokerC = PeopleBroker(DB_URL)

        def shutEmDown(null):
            dList = []
            for broker in (self.broker, brokerB, brokerC):
                dList.append(
                    broker.shutdown().addCallback(
                        self._oneShutdown, broker))
            return defer.DeferredList(dList)

        d = defer.DeferredList([brokerB.startup(), brokerC.startup()])
        d.addCallback(shutEmDown)
        return d

    def test_connect(self):
        def gotAll(null):
            prevItem = mutable.pop()
            while mutable:
                thisItem = mutable.pop()
                # Both should be connections, not necessarily the same
                # one
                self.failUnlessEqual(type(thisItem), type(prevItem))
                prevItem = thisItem

        mutable = []
        d1 = self.broker.connect().addCallback(mutable.append)
        d2 = self.broker.connect().addCallback(mutable.append)
        d3 = deferToDelay(DELAY).addCallback(
            lambda _: self.broker.connect()).addCallback(mutable.append)
        return defer.DeferredList([d1, d2, d3]).addCallback(gotAll)

    @defer.inlineCallbacks
    def test_twoConnections(self):
        firstConnection = yield self.broker.connect()
        yield self.broker.shutdown()
        self.broker = PeopleBroker(DB_URL)
        secondConnection = yield self.broker.connect()
        self.failUnlessEqual(type(firstConnection), type(secondConnection))

    def test_sameUrlSameQueueNotStarted(self):
        anotherBroker = PeopleBroker(DB_URL)
        self.failUnlessEqual(self.broker.q, anotherBroker.q)
        d1 = anotherBroker.shutdown()
        d2 = self.broker.shutdown()
        return defer.DeferredList([d1,d2])

    @defer.inlineCallbacks
    def test_sameUrlSameQueueStarted(self):
        yield deferToDelay(DELAY)
        anotherBroker = PeopleBroker(DB_URL, verbose=True)
        self.failUnlessEqual(self.broker.q, anotherBroker.q)
        yield anotherBroker.shutdown()

    @defer.inlineCallbacks
    def test_deferToQueue_errback(self):
        anotherBroker = PeopleBroker(DB_URL, returnFailure=True)
        d = anotherBroker.deferToQueue(lambda x: 1/0, 0)
        d.addCallbacks(
            lambda _: self.fail("Should have done the errback instead"),
            self.assertIsFailure)
        yield d
        yield anotherBroker.shutdown()

    @defer.inlineCallbacks
    def test_transact_errback(self):
        anotherBroker = PeopleBroker(DB_URL, returnFailure=True)
        d = anotherBroker.erroneousTransaction()
        d.addCallbacks(
            lambda _: self.fail("Should have done the errback instead"),
            self.assertIsFailure)
        yield d
        yield anotherBroker.shutdown()


class TestTables(BrokerTestCase):
    verbose = True

    def _tableList(self):
        def rpReady(rp):
            return rp.fetchall()
        self.broker.sql("SHOW TABLES").addCallback(rpReady)
        
    @defer.inlineCallbacks
    def test_table(self):
        yield self.broker.makeFoobarTable()
        tables = yield self._tableList()
        for tableName in ('people', 'foobars'):
            self.assertIn(tableName, tables)

    @defer.inlineCallbacks
    def test_createTableWithIndex(self):
        yield self.broker.table(
            'table_indexed',
            SA.Column('id', SA.Integer, primary_key=True),
            SA.Column('foo', SA.String(32)),
            SA.Column('bar', SA.String(64)),
            index_foobar=['foo', 'bar'])
        tables = yield self._tableList()
        self.assertIn('table_indexed', tables)
        yield self.broker.sql("DROP TABLE table_indexed")

    @defer.inlineCallbacks
    def test_createTableWithUnique(self):
        yield self.broker.table(
            'table_unique',
            SA.Column('id', SA.Integer, primary_key=True),
            SA.Column('foo', SA.String(32)),
            SA.Column('bar', SA.String(64)),
            unique_foobar=['foo', 'bar'])
        tables = yield self._tableList()
        self.assertIn('table_unique', tables)
        yield self.broker.sql("DROP TABLE table_unique")

    @defer.inlineCallbacks
    def test_makeNewTable(self):
        yield self.broker.makeFoobarTable().addErrback(self.oops)
        table = self.broker.foobars
        isType = str(type(table))
        self.assertPattern(
            r'sqlalchemy.+[tT]able',
            "AccessBroker().foobars should be an sqlalchemy Table() "+\
            "object, but is '{}'".format(isType))


class TestTransactions(BrokerTestCase):
    verbose = True
    spew = True

    def test_selectOneAndTwoArgs(self):
        def runInThread():
            self.failUnlessEqual(s('thisSelect'), False)
            s([self.broker.people], self.broker.people.c.id==1)
            self.failUnlessEqual(s('thisSelect'), True)
            self.failUnlessEqual(s('thatSelect'), False)
        s = self.broker.s
        return self.broker.q.call(runInThread).addErrback(self.oops)

    @defer.inlineCallbacks
    def test_selectZeroArgs(self):
        def runInThread():
            s('roosevelts')
            s([self.broker.people],
              self.broker.people.c.name_last == 'Roosevelt')
            return s().execute().fetchall()
        s = self.broker.s
        yield self.broker.q.call(runInThread)
        rows = yield self.broker.q.call(runInThread).addErrback(self.oops)
        nameList = [row[self.broker.people.c.name_first] for row in rows]
        for lastName in ('Franklin', 'Theodore'):
            self.failUnless(lastName in nameList)

    @defer.inlineCallbacks
    def test_iterate(self):
        dr = yield self.broker.everybody()
        self.assertIsInstance(dr, iteration.Deferator)
        rows = []
        for k, d in enumerate(dr):
            row = yield d
            self.msg("Row #{:d}: {}", k+1, row)
            self.assertNotIn(row, rows)
            rows.append(row)
        self.assertEqual(len(rows), 5)
    
    @defer.inlineCallbacks
    def test_iterate_withConsumer(self):
        consumer = IterationConsumer(self.verbose)
        yield self.broker.everybody(consumer=consumer)
        self.assertEqual(len(consumer.data), 5)
        
    @defer.inlineCallbacks
    def test_iterate_nextWhileIterating(self):
        slowConsumer = IterationConsumer(self.verbose, writeTime=0.2)
        # In this case, do NOT wait for the done-iterating deferred
        # before doing another transaction
        d = self.broker.everybody(consumer=slowConsumer)
        # Add a new person while we are iterating the people from the
        # last query
        yield self.broker.addPerson("George", "Washington")
        # Confirm we have one more person now
        fastConsumer = IterationConsumer(self.verbose)
        yield self.broker.everybody(consumer=fastConsumer)
        self.assertEqual(len(fastConsumer.data), 6)
        # Now wait for the slow consumer
        yield d
        # It still should only have gotten the smaller number of people
        self.assertEqual(len(slowConsumer.data), 5)
        # Wait for the slow consumer's last write delay, just to avoid
        # unclean reactor messiness
        yield slowConsumer.d

    @defer.inlineCallbacks
    def test_selex_select(self):
        cols = self.broker.people.c
        with self.broker.selex(cols.name_first) as sh:
            sh.where(cols.name_last == 'Luther')
        rp = yield sh(raw=True)
        row = rp.first()
        self.assertEqual(row[0], 'Martin')

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

    @defer.inlineCallbacks        
    def test_selectorator(self):
        cols = self.broker.people.c
        s = self.broker.select([cols.name_last, cols.name_first])
        dr = yield self.broker.selectorator(s)
        rows = []
        for k, d in enumerate(dr):
            row = yield d
            self.msg("Row #{:d}: {}", k+1, row)
            self.assertNotIn(row, rows)
            rows.append(row)
        self.assertEqual(len(rows), 5)

    @defer.inlineCallbacks
    def test_selectorator_withConsumer(self):
        consumer = IterationConsumer(self.verbose)
        cols = self.broker.people.c
        s = self.broker.select([cols.name_last, cols.name_first])
        yield self.broker.selectorator(s, consumer)
        self.assertEqual(len(consumer.data), 5)

    @defer.inlineCallbacks
    def test_selectorator_twoConcurrently(self):
        slowConsumer = IterationConsumer(self.verbose, writeTime=0.2)
        cols = self.broker.people.c
        # In this case, do NOT wait for the done-iterating deferred
        # before doing another selectoration
        s = self.broker.select([cols.name_last, cols.name_first])
        d = self.broker.selectorator(s, slowConsumer)
        # Add a new person while we are iterating the people from the
        # last query
        yield self.broker.addPerson("George", "Washington")
        # Confirm we have one more person now
        fastConsumer = IterationConsumer(self.verbose)
        yield self.broker.everybody(consumer=fastConsumer)
        self.assertEqual(len(fastConsumer.data), 6)
        # Now wait for the slow consumer
        yield d
        # It still should only have gotten the smaller number of people
        self.assertEqual(len(slowConsumer.data), 5)
        # Wait for the slow consumer's last write delay, just to avoid
        # unclean reactor messiness
        yield slowConsumer.d

    @defer.inlineCallbacks
    def test_transactMany(self):
        dL = []
        for letter in "htudg":
            d = self.broker.matchingNames(letter)
            dL.append(d)
        yield defer.DeferredList(dL)
        print self.broker.matches
        self.assertEqual(self.broker.matches['Theodore'], ['h', 'd'])

    def test_transactionAutoStartup(self):
        d = self.broker.fakeTransaction(1)
        d.addCallback(self.failUnlessEqual, 2)
        return d

    def test_nestTransactions(self):
        d = self.broker.nestedTransaction(1)
        d.addCallback(self.failUnlessEqual, 3)
        return d
        
        
