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
Asynchronous database transactions via SQLAlchemy.
"""

import sys, logging
from contextlib import contextmanager

from twisted.internet import defer, reactor
from twisted.python.failure import Failure

import sqlalchemy as SA
from sqlalchemy import pool

import asynqueue
from asynqueue import iteration

import queue
from selex import SelectAndResultHolder


def nextFromRP(rp):
    try:
        row = rp.fetchone()
    except:
        row = None
    if row:
        return row
    raise StopIteration


def transact(f):
    """
    Use this function as a decorator to wrap the supplied method I{f}
    of L{AccessBroker} in a transaction that runs C{f(*args, **kw)} in
    its own transaction.

    Immediately returns an instance of
    L{twisted.internet.defer.Deferred} that will eventually have its
    callback called with the result of the transaction. Inspired by
    and largely copied from Valentino Volonghi's C{makeTransactWith}
    code.

    You can add the following keyword options to your function call:

    @keyword niceness: Scheduling niceness, an integer between -20 and
      20, with lower numbers having higher scheduling priority as in
      UNIX C{nice} and C{renice}.

    @keyword doNext: Set C{True} to assign highest possible priority,
      even higher than with niceness = -20.

    @keyword doLast: Set C{True} to assign lower possible priority,
      even lower than with niceness = 20.

    @keyword ignore: Set this option to C{True} to have errors in the
      transaction function ignored and just do the rollback quietly.

    @type ignore: Boolean option, default C{False}

    @keyword consumer: Set this to a consumer object (must implement
      the L{twisted.interfaces.IConsumer} interface) and the
      L{SA.ResultProxy} will write its rows to it in Twisted
      fashion. The returned deferred will fire when all rows have been
      written.
    """
    @defer.inlineCallbacks
    def substituteFunction(self, *args, **kw):
        """
        Puts the original function in the synchronous task queue and
        returns a deferred to its result when it is eventually run.

        If the transaction resulted in a ResultProxy object, the
        deferred fires with an L{iteration.Deferator}, unless you've
        supplied an IConsumer with the 'consumer' keyword. Then the
        deferred result is an IterationProducer coupled to your
        consumer.

        This function will be given the same name as the original
        function so that it can be asked to masquerade as the original
        function. As a result, the threaded call to the original
        function that it makes inside its C{transaction} sub-function
        will be able to use the arguments for that original
        function. (The caller will actually be calling this substitute
        function, but it won't know that.)

        The original function should be a method of a L{AccessBroker}
        subclass instance, and the queue for that instance will be
        used to run it.
        """
        def transaction(func, *t_args, **t_kw):
            """
            Everything making up a transaction, and everything run in the
            thread, is contained within this little function,
            including of course a call to C{func}.
            """
            trans = self.connection.begin()
            if not hasattr(func, 'im_self'):
                t_args = (self,) + t_args
            try:
                result = func(*t_args, **t_kw)
            except Exception as e:
                trans.rollback()
                if ignore:
                    return
                return Failure(e)
            # We can commit and release the lock now
            trans.commit()
            return result

        def isNested():
            frame = sys._getframe()
            while True:
                frame = frame.f_back
                if frame is None:
                    return False
                if frame.f_code == transaction.func_code:
                    return True

        def oops(failureObj):
            # Encapsulate the failure object in a list to avoid the
            # errback chain until we are ready.
            return [failureObj]

        ignore = kw.pop('ignore', False)
        consumer = kw.pop('consumer', None)
        if isNested():
            # The call and its result only get special treatment in
            # the outermost @transact function
            result = f(self, *args, **kw)
        else:
            # Here's where the ThreadQueue actually runs the
            # transaction
            if self.singleton or not self.running:
                # Not yet running, "wait" here for queue, engine, and
                # connection
                yield self.lock.acquire()
                if not self.singleton:
                    # If we can handle multiple connections (TODO), we
                    # don't want to hold onto the lock because
                    # transactions are queued in the ThreadQueue and
                    # additional connections can be obtained when ding
                    # ResultProxy iteration.
                    self.lock.release()
            result = yield self.q.call(
                transaction, f, *args, **kw).addErrback(oops)
            if getattr(result, 'returns_rows', False):
                # A ResultsProxy gets special handling
                result = yield self.handleResultsProxy(result, consumer)
            if self.singleton:
                # If we can't handle multiple connections, we held
                # onto the lock throughout all of this
                self.lock.release()
        # If the result is a failure, raise its exception to trigger
        # the errback outside this function
        if isinstance(result, list) and \
           len(result) == 1 and isinstance(result[0], Failure):
            result[0].raiseException()
        defer.returnValue(result)

    if f.func_name == 'first' and hasattr(f, 'im_self'):
        return f
    substituteFunction.func_name = f.func_name
    return substituteFunction


class AccessBroker(object):
    """
    I manage asynchronous access to a database.

    Before you use any instance of me, you must specify the parameters
    for creating an SQLAlchemy database engine. A single argument is
    used, which specifies a connection to a database via an RFC-1738
    url. In addition, the following keyword options can be employed,
    which are listed below with their default values.

    You can set an engine globally, for all instances of me via the
    L{sasync.engine} package-level function, or via my L{engine} class
    method. Alternatively, you can specify an engine for one
    particular instance by supplying the parameters to the
    constructor.

    Because I employ AsynQueue to queue up transactions asynchronously
    and perform them one at a time, I don't need or want a connection
    pool for my database engine.
          
    SQLAlchemy has excellent documentation, which describes the engine
    parameters in plenty of detail. See
    U{http://www.sqlalchemy.org/docs/dbengine.myt}.

    @ivar q: A property-generated reference to a threaded task queue
      that is dedicated to my database connection.

    @ivar connection: The current SQLAlchemy connection object, if
      any yet exists. Generated by my L{connect} method.
    """
    # TODO: Allow for additional connections for transactions received
    # while doing ResultProxy iteration from a previous one. Until
    # then, this is always True
    singleton = True
    # A single class-wide queue factory
    qFactory = queue.Factory()

    @classmethod
    def setup(cls, url, **kw):
        """
        Constructs a global queue for all instances of me, returning a
        deferred that fires with it.
        """
        return cls.qFactory.setGlobal(url, **kw)
    
    def __init__(self, *args, **kw):
        """
        Constructs an instance of me, optionally specifying parameters for
        an SQLAlchemy engine object that serves this instance only.
        """
        def firstAsTransaction():
            with self.connection.begin():
                self.first()
        
        @defer.inlineCallbacks
        def startup(null):
            # Queue with attached engine, possibly shared with other
            # AccessBrokers
            self.q = yield self.qFactory(*args, **kw)
            # A connection of my very own
            self.connection = yield self.connect()
            # Pre-transaction startup, called in main loop after
            # connection made.
            yield defer.maybeDeferred(self.startup)
            # First transaction, called in thread
            yield self.q.deferToThread(firstAsTransaction)
            # Ready for regular transactions
            self.running = True
            self.lock.release()
            reactor.addSystemEventTrigger(
                'before', 'shutdown', self.shutdown)
        
        self.selects = {}
        self.rowProxies = []
        self.running = False
        # The deferred lock lets us easily wait until setup is done
        # and avoids running multiple transactions at once when they
        # aren't wanted.
        self.lock = asynqueue.DeferredLock()
        self.lock.acquire().addCallback(startup)

    def connect(self):
        def nowConnect(null):
            return self.q.call(
                self.q.engine.contextual_connect)
        if not getattr(self, 'q', None):
            return self.waitUntilRunning().addCallback(nowConnect)
        return nowConnect(None)

    @defer.inlineCallbacks
    def waitUntilRunning(self):
        if not self.running:
            yield self.lock.acquire()
            self.lock.release()

    def callWhenRunning(self, f, *args, **kw):
        return self.waitUntilRunning().addCallback(lambda _: f(*args, **kw))

    @defer.inlineCallbacks
    def table(self, name, *cols, **kw):
        """
        Instantiates a new table object, creating it in the transaction
        thread as needed.

        One or more indexes other than the primary key can be defined
        via a keyword prefixed with I{index_} or I{unique_} and having
        the index name as the suffix. Use the I{unique_} prefix if the
        index is to be a unique one. The value of the keyword is a
        list or tuple containing the names of all columns in the
        index.
        """
        def haveQueue():
            return getattr(self, 'q', None)
        
        def makeTable():
            if not hasattr(self, '_meta'):
                self._meta = SA.MetaData(self.q.engine)
            indexes = {}
            for key in kw.keys():
                if key.startswith('index_'):
                    unique = False
                elif key.startswith('unique_'):
                    unique = True
                else:
                    continue
                indexes[key] = kw.pop(key), unique
            kw.setdefault('useexisting', True)
            table = SA.Table(name, self._meta, *cols, **kw)
            table.create(checkfirst=True)
            setattr(self, name, table)
            return table, indexes

        def makeIndex(tableInfo):
            table, indexes = tableInfo
            for key, info in indexes.iteritems():
                kwIndex = {'unique':info[1]}
                try:
                    # This is stupid. Why can't I see if the index
                    # already exists and only create it if needed?
                    index = SA.Index(
                        key, *[
                            getattr(table.c, x) for x in info[0]], **kwIndex)
                    index.create()
                except:
                    pass

        if not hasattr(self, name):
            if not haveQueue():
                # This is tricky; startup hasn't finished, but making
                # a table is likely to be part of the startup. What we
                # really need to wait for is the presence of a queue.
                yield iteration.Delay(backoff=1.02).untilEvent(haveQueue)
            tableInfo = yield self.q.deferToThread(makeTable)
            yield self.q.deferToThread(makeIndex, tableInfo)
    
    def startup(self):
        """
        This method runs before the first transaction to start my
        synchronous task queue. B{Override it} to get whatever
        pre-transaction stuff you have run in the main loop before a
        database engine/connection is created.
        """
        return defer.succeed(None)

    def first(self):
        """
        This method automatically runs as the first transaction after
        completion of L{startup}. B{Override it} to define table
        contents or whatever else you want as a first transaction that
        immediately follows your pre-transaction stuff.

        You don't need to decorate the method with C{@transact}, but
        it doesn't break anything if you do.
        """

    @defer.inlineCallbacks
    def shutdown(self, *null):
        """
        Shuts down my database transaction functionality and threaded task
        queue, returning a deferred that fires when all queued tasks
        are done and the shutdown is complete.
        """
        def closeConnection():
            conn = getattr(self, 'connection', None)
            if conn is not None:
                if hasattr(conn, 'connection'):
                    # Close the raw DBAPI connection rather than a
                    # proxied one. Does this actually make any
                    # difference?
                    conn = conn.connection
                conn.close()

        if self.running:
            yield self.lock.acquire()
            yield self.q.call(closeConnection)
            self.running = False
            self.lock.release()
            yield self.qFactory.kill(self.q)

    def handleResultsProxy(self, rp, consumer=None, connection=None):
        """
        Given a ResultsProxy and possibly an implementor of IConsumer,
        returned a (deferred) instance of Deferator or couples your
        consumer to an IterationProducer.
        """
        def closeConnection():
            if connection:
                connection.close()
        
        def pfReady(ok):
            if not ok:
                # Prefetcherator wouldn't accept it (will this ever
                # happen)?
                return Failure(Exception(
                    "Prefetcherator rejected results proxy!"))
            dr = iteration.Deferator(pf)
            if consumer:
                # A consumer was supplied, so try to make an
                # IterationProducer couple to it
                ip = iteration.IterationProducer(dr, consumer)
                return ip.run().addCallback(lambda _: consumer)
            # No consumer supplied, just return the Deferator
            return dr
        pf = iteration.Prefetcherator(repr(rp), closeConnection)
        return pf.setup(
            self.q.deferToThread, nextFromRP, rp).addCallback(pfReady)
            
    def s(self, *args, **kw):
        """
        Polymorphic method for working with C{select} instances within a
        cached selection subcontext.

        - When called with a single argument (the select object's name
          as a string) and no keywords, this method indicates if the
          named select object already exists and sets its selection
          subcontext to I{name}.
            
        - With multiple arguments or any keywords, the method acts
          like a call to C{sqlalchemy.select(...).compile()}, except
          that nothing is returned. Instead, the resulting select
          object is stored in the current selection subcontext.
            
        - With no arguments or keywords, the method returns the select
          object for the current selection subcontext.

        Call from inside a transaction.

        """
        if kw or (len(args) > 1):
            # It's a compilation.
            context = getattr(self, 'context', None)
            self.selects[context] = SA.select(*args, **kw).compile()
        elif len(args) == 1:
            # It's a lookup to see if the select has been previously
            # seen and compiled; return True or False.
            self.context = args[0]
            return self.context in self.selects
        else:
            # It's a retrieval of a compiled selection object, keyed off
            # the most recently mentioned context.
            context = getattr(self, 'context', None)
            return self.selects.get(context)

    def select(self, *args, **kw):
        """
        Just returns an SQLAlchemy select object. You do everything else.
        """
        return SA.select(*args, **kw)
            
    @contextmanager
    def selex(self, *args, **kw):
        """
        Supply columns as arguments and this method generates a select on
        the columns, yielding a placeholder object with the same
        attributes as the select object itself.

        Supply a callable as an argument (along with any of its args)
        and it yields a placeholder whose attributes are the same as
        the result of that call.

        In either case, you do stuff with the placeholder and call it
        to execute the connection with it. Supply the name of a
        resultsproxy method (and any of its args) to the call to get
        the result instead of the rp. Do all of this within the
        context of the placeholder:

        with <me>.select(<table>.c.foo) as sh:
            sh.where(<table>.c.bar == "correct")
            rows = sh().fetchall()
        <proceed with rows...>

        Call from inside a transaction.
        
        """
        sh = SelectAndResultHolder(self.connection, *args)
        yield sh
        sh.close()

    @defer.inlineCallbacks
    def selectorator(self, selectObj, consumer=None):
        """
        When called with a select object that results in an iterable
        ResultProxy when executed, returns a deferred that fires with
        a Deferator that can be iterated over deferreds, each of which
        fires with a successive row of the select's ResultProxy.

        If you supply an IConsumer with the 'consumer' keyword, I will
        couple an L{iteration.IterationProducer} to your consumer and
        run it. The returned deferred will fire (with a reference to
        your consumer) when the iterations are done, but you don't
        need to wait for that before doing another transaction.
        
        Call directly, *not* from inside a transaction.

        dr = yield <me>.selectorator(<select>)
        for d in dr:
            row = yield d
            <proceed with row>

        d = <me>.selectorator(<select>, <consumer>)
        <do other stuff>
        consumer = yield d
        consumer.revealMagnificentSummary()
        
        """
        yield self.waitUntilRunning()
        # A new connection just for this iteration, so that other
        # transactions can (hopefully) proceed while iteration is
        # happening.
        connection = yield self.connect()
        rp = yield self.q.call(connection.execute, selectObj)
        result = yield self.handleResultsProxy(rp, consumer, connection)
        defer.returnValue(result)

    def deferToQueue(self, func, *args, **kw):
        """
        Dispatches I{callable(*args, **kw)} as a task via the like-named
        method of my asynchronous queue, returning a deferred to its
        eventual result.

        Scheduling of the call is impacted by the I{niceness} keyword
        that can be included in I{**kw}. As with UNIX niceness, the
        value should be an integer where 0 is normal scheduling,
        negative numbers are higher priority, and positive numbers are
        lower priority.
        
        @keyword niceness: Scheduling niceness, an integer between -20
          and 20, with lower numbers having higher scheduling priority
          as in UNIX C{nice} and C{renice}.
        """
        return self.waitUntilRunning().addCallback(
            lambda _: self.q.call(func, *args, **kw))


__all__ = ['transact', 'AccessBroker', 'SA']

