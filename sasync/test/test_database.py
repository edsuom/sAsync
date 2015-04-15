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
from testbase import deferToDelay, TestHandler, IterationConsumer, TestCase

from database import transact


VERBOSE = False

DELAY = 0.5
DB_URL = 'mysql://test@localhost/test'
#DB_URL = 'sqlite://'


class FakeConnection:
    def __init__(self):
        self.wasClosed = False
    def close(self):
        self.wasClosed = True


class BrokerTestCase(TestCase):
    verbose = False
    spew = False

    def brokerFactory(self, **kw):
        if 'verbose' not in kw:
            kw['verbose'] = self.isVerbose()
        if 'spew' not in kw:
            kw['spew'] = self.spew
        return PeopleBroker(DB_URL, **kw)
    
    def setUp(self):
        self.handler = TestHandler(self.isVerbose())
        logging.getLogger('asynqueue').addHandler(self.handler)
        self.broker = self.brokerFactory()
        return self.broker.waitUntilRunning()
        
    @defer.inlineCallbacks
    def tearDown(self):
        if getattr(getattr(self, 'broker', None), 'running', False):
            for tableName in ('people', 'foobars'):
                if hasattr(self.broker, tableName):
                    yield self.broker.sql("DROP TABLE {}".format(tableName))
            yield self.broker.shutdown()

            
class TestBasics(BrokerTestCase):
    verbose = False
    spew = False

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
        def shutEmDown(null):
            dList = []
            for broker in (self.broker, anotherBroker):
                dList.append(
                    broker.shutdown().addCallback(
                        self._oneShutdown, broker))
            return defer.DeferredList(dList)
        anotherBroker = self.brokerFactory()
        return anotherBroker.waitUntilRunning().addCallback(shutEmDown)

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
        self.broker = self.brokerFactory()
        secondConnection = yield self.broker.connect()
        self.failUnlessEqual(type(firstConnection), type(secondConnection))

    @defer.inlineCallbacks
    def test_sameUrlSameQueue(self):
        anotherBroker = self.brokerFactory()
        yield anotherBroker.waitUntilRunning()
        self.assertEqual(self.broker.q, anotherBroker.q)
        # The shutdown from one broker MUST be completed before any
        # other is tried.
        yield anotherBroker.shutdown()
        yield self.broker.shutdown()

    @defer.inlineCallbacks
    def test_deferToQueue_errback(self):
        anotherBroker = self.brokerFactory(returnFailure=True)
        d = anotherBroker.deferToQueue(lambda x: 1/0, 0)
        d.addCallbacks(
            lambda _: self.fail("Should have done the errback instead"),
            self.assertIsFailure)
        yield d
        yield anotherBroker.shutdown()

    @defer.inlineCallbacks
    def test_transact_errback(self):
        anotherBroker = self.brokerFactory(returnFailure=True)
        d = anotherBroker.erroneousTransaction()
        d.addCallbacks(
            lambda _: self.fail("Should have done the errback instead"),
            self.assertIsFailure)
        yield d
        yield anotherBroker.shutdown()

    @defer.inlineCallbacks
    def test_handleResult_asList(self):
        def getResultToHandle():
            col = self.broker.people.c
            s = SA.select([col.name_first, col.name_last])
            return s.execute()
        rp = yield self.broker.deferToQueue(getResultToHandle)
        rowList = yield self.broker.handleResult(rp, asList=True)
        self.assertEqual(len(rowList), 5)
        for row in rowList:
            self.assertIn(row, self.broker.defaultRoster)
        
    @defer.inlineCallbacks
    def test_handleResult_asDeferator(self):
        def getResultToHandle():
            col = self.broker.people.c
            s = SA.select([col.name_first, col.name_last])
            return s.execute()
        fc = FakeConnection()
        rp = yield self.broker.deferToQueue(getResultToHandle)
        dr = yield self.broker.handleResult(rp, conn=fc)
        for k, d in enumerate(dr):
            row = yield d
            self.assertIn(row, self.broker.defaultRoster)
        self.assertEqual(k, 4)
        self.assertTrue(fc.wasClosed)

    @defer.inlineCallbacks
    def test_handleResult_asProducer(self):
        def getResultToHandle():
            col = self.broker.people.c
            s = SA.select([col.name_first, col.name_last])
            return s.execute()
        fc = FakeConnection()
        rp = yield self.broker.deferToQueue(getResultToHandle)
        consumer = IterationConsumer(self.verbose, 0.05)
        yield self.broker.handleResult(rp, consumer=consumer, conn=fc)
        yield consumer.d
        for k, row in enumerate(consumer.data):
            self.assertIn(row, self.broker.defaultRoster)
        self.assertEqual(k, 4)
        self.assertTrue(fc.wasClosed)
    
    @defer.inlineCallbacks
    def test_handleResult_empty(self):
        def getResultToHandle():
            col = self.broker.people.c
            s = SA.select(
                [col.name_first, col.name_last]).where(
                    col.name_first == 'Impossible')
            return s.execute()
        rp = yield self.broker.deferToQueue(getResultToHandle)
        result = yield self.broker.handleResult(rp)
        self.assertEqual(result, [])
            

class TestTables(BrokerTestCase):
    verbose = False
    spew = False

    def _tableList(self):
        """
        Adapted from
        https://www.mail-archive.com/sqlalchemy@googlegroups.com/msg00462.html
        """
        def done(rows):
            return [x[0] for x in rows]
        
        eng = self.broker.q.engine
        if eng.name == 'sqlite':
            sql = """
            SELECT name FROM sqlite_master
            WHERE type='table'
            ORDER BY name;"""
        elif eng.name == 'postgres':
            sql = """
            SELECT c.relname as name,
              n.nspname as schema,c.relkind,
              u.usename as owner
            FROM pg_catalog.pg_class c
              LEFT JOIN pg_catalog.pg_user u ON u.usesysid = c.relowner
              LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('r') AND pg_catalog.pg_table_is_visible(c.oid)
            ORDER BY 1,2;
            """
        elif eng.name == 'mysql':
            sql = "SHOW TABLES"
        return self.broker.sql(sql).addCallback(done)
        
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
    verbose = False
    spew = False

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
        row = yield sh(asList=True)
        self.assertEqual(row[0][0], 'Martin')

    @defer.inlineCallbacks
    def test_selex_delete(self):
        table = self.broker.people
        with self.broker.selex(table.delete) as sh:
            sh.where(table.c.name_last == 'Luther')
        rp = yield sh(raw=True)
        N = rp.rowcount
        self.assertGreater(N, 0)

    def test_selex_nested(self):
        def gotMembers(members):
            self.assertIn("Franklin", members)
            self.assertIn("Theodore", members)
        return self.broker.familyMembers("Roosevelt").addCallback(gotMembers)
        
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
        consumer = yield self.broker.selectorator(s, consumer)
        self.assertEqual(len(consumer.data), 5)

    @defer.inlineCallbacks
    def test_selectorator_twoConcurrently(self):
        slowConsumer = IterationConsumer(self.verbose, writeTime=0.2)
        cols = self.broker.people.c
        # In this case, do NOT wait for the done-iterating deferred
        # before doing another selectoration
        dSelectExecuted = defer.Deferred()
        s = self.broker.select([cols.name_last, cols.name_first])
        d = self.broker.selectorator(s, slowConsumer, dSelectExecuted)
        # Wait until the query was executed...
        yield dSelectExecuted
        # ...then add a new person while we are iterating the people
        # from that query
        yield self.broker.addPerson("George", "Washington")
        # Confirm we have one more person now
        fastConsumer = IterationConsumer(self.verbose)
        yield self.broker.everybody(consumer=fastConsumer)
        #self.assertEqual(len(fastConsumer.data), 6)
        # Now wait for the slow consumer
        yield d
        # It still should only have gotten the smaller number of people
        self.assertEqual(len(slowConsumer.data), 5)
        # Wait for the slow consumer's last write delay, just to avoid
        # unclean reactor messiness
        yield slowConsumer.d

    def test_transactionAutoStartup(self):
        d = self.broker.fakeTransaction(1)
        d.addCallback(self.failUnlessEqual, 2)
        return d

    def test_nestTransactions(self):
        d = self.broker.nestedTransaction(1)
        d.addCallback(self.failUnlessEqual, 3)
        return d
        
        
