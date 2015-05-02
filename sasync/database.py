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

"""
Asynchronous database transactions via C{SQLAlchemy} and
C{Twisted}. You will surely have a subclass of L{AccessBroker}.
"""

import inspect, logging
from contextlib import contextmanager

from twisted.internet import defer, reactor
from twisted.python.failure import Failure

import sqlalchemy as SA
from sqlalchemy import pool

import asynqueue
from asynqueue import iteration

import errors, queue
from selex import SelectAndResultHolder

def transaction(self, func, *t_args, **t_kw):
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
        text = asynqueue.Info().setCall(
            func, t_args, t_kw).aboutException(exception=e)
        raise errors.TransactionError(text)
    else:
        trans.commit()
        return result

def nextFromRP(rp, N=1):
    """
    I{Transaction magic.}
    """
    try:
        if N == 1:
            result = rp.fetchone()
        else:
            result = rp.fetchmany(N)
    except:
        result = []
    if result:
        return result
    raise StopIteration
    
def isNested(self):
    """
    I{Transaction magic.}
    """
    firstCode = None
    frame = inspect.currentframe()
    while True:
        frame = frame.f_back
        if frame is None:
            result = False
            break
        if frame.f_code == transaction.func_code:
            result = True
            break
        # Check if nested inside first-transaction code if necessary
        if firstCode is None and hasattr(self, 'first'):
            firstCode = getattr(self.first, 'func_code', False)
        if firstCode and frame.f_code == firstCode:
            result = True
            break
    del frame
    del firstCode
    return result


def transact(f):
    """
    Transaction decorator.
    
    Use this function as a decorator to wrap the supplied method I{f}
    of L{AccessBroker} in a transaction that runs C{f(*args, **kw)} in
    its own transaction.

    Immediately returns a C{Deferred} that will eventually have its
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

    @keyword consumer: Set this to a consumer object (must implement
      the L{twisted.interfaces.IConsumer} interface) and the
      L{SA.ResultProxy} will write its rows to it in Twisted
      fashion. The returned deferred will fire when all rows have been
      written.

    @keyword raw: Set C{True} to have the transaction result returned
      in its original form even if it's a RowProxy or other iterator.
    """
    def substituteFunction(self, *args, **kw):
        """
        Puts the original function in the synchronous task queue and
        returns a deferred to its result when it is eventually run.

        If the transaction resulted in a C{ResultProxy} object, the
        C{Deferred} fires with an L{asynqueue.iteration.Deferator},
        unless you've supplied an IConsumer with the I{consumer}
        keyword. Then the deferred result is an
        L{asynqueue.iteration.IterationProducer} coupled to your
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

        @see: U{/AsynQueue/asynqueue.iteration.html}
        """
        def oops(failureObj):
            # Encapsulate the failure object in a list to avoid the
            # errback chain until we are ready.
            return [failureObj]

        @defer.inlineCallbacks
        def doTransaction():
            if self.singleton or not self.running:
                # Not yet running, "wait" here for queue, engine, and
                # connection
                yield self.lock.acquire()
                if not self.singleton:
                    # If we can handle multiple connections (TODO), we
                    # don't want to hold onto the lock because
                    # transactions are queued in the ThreadQueue and
                    # additional connections can be obtained when doing
                    # ResultProxy iteration.
                    self.lock.release()
            result = yield self.q.call(
                transaction, self, f, *args, **kw).addErrback(oops)
            if not raw:
                result = yield self.handleResult(
                    result, consumer=consumer, asList=asList)
            if self.singleton:
                # If we can't handle multiple connections, we held
                # onto the lock throughout all of this
                self.lock.release()
            # If the result is a failure, raise its exception to trigger
            # the errback outside this function. Very weird, I
            # know. Basically, there must be an exception IN this function
            # to trigger the errback OUTSIDE of it. Just returning a
            # Failure doesn't seem to work.
            if isinstance(result, list):
                if len(result) == 1 and isinstance(result[0], Failure):
                    result[0].raiseException()
            defer.returnValue(result)

        # The 'raw' keyword is also used by the queue/worker, but
        # 'asList', 'isNested', and 'consumer' are not.
        raw = kw.get('raw', False)
        asList = kw.pop('asList', False)
        consumer = kw.pop('consumer', None)
        if consumer and raw:
            raise ValueError(
                "Can't supply a consumer for a raw transaction result")
        if kw.pop('isNested', False) or isNested(self):
            # The call and its result only get special treatment in
            # the outermost @transact function
            return f(self, *args, **kw)
        # Here's where the ThreadQueue actually runs the
        # transaction
        return doTransaction()

    # We need to ignore any transact decorator on an AccessBroker's
    # 'first' method, because it can't wait for the lock
    if f.func_name == 'first':
        return f
    substituteFunction.func_name = f.func_name
    return substituteFunction


class AccessBroker(object):
    """
    I manage asynchronous access to a database.

    Before you use any instance of me, you must specify the parameters
    for creating an SQLAlchemy database engine. A single argument is
    used, which specifies a connection to a database via an RFC-1738
    url. In addition, the I{verbose} keyword options can be employed
    to spew out log messages about calls and errors. (Use that only
    for debugging.)

    You can set an engine globally, for all instances of me via the
    L{sasync.engine} package-level function, or via my L{engine} class
    method. Alternatively, you can specify an engine for one
    particular instance by supplying the parameters to the
    constructor.

    I employ C{AsynQueue} to queue up transactions asynchronously and
    perform them one at a time. But I still want a connection pool for
    my database engine to handle asynchronous iteration of rows from
    query results. See the source for L{handleResult}.
          
    SQLAlchemy has excellent documentation, which describes the engine
    parameters in plenty of detail. See
    U{http://docs.sqlalchemy.org/en/rel_1_0/core/engines.html}.

    @ivar q: A property-generated reference to a threaded task queue
      that is dedicated to my database connection.

    @ivar connection: The current SQLAlchemy connection object, if
      any yet exists. Generated by my L{connect} method.

    @see: U{http://edsuom.com/AsynQueue/asynqueue.threads.ThreadQueue.html}
    """
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
            # Need to call the first transaction here rather than via
            # a regular 'transact' substitute call to avoid competing
            # for the lock
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

    @property
    def singleton(self):
        if not hasattr(self, '_singleton'):
            engine = getattr(getattr(self, 'q', None), 'engine', None)
            self._singleton = getattr(engine, 'name', 'sqlite') == 'sqlite'
        return self._singleton
    
    def connect(self):
        def nowConnect(null):
            return self.q.call(
                self.q.engine.contextual_connect)
        if not getattr(self, 'q', None):
            return self.waitUntilRunning().addCallback(nowConnect)
        return nowConnect(None)

    @defer.inlineCallbacks
    def waitUntilRunning(self):
        """
        Returns a C{Deferred} that fires when the broker is running and
        ready for transactions.
        """
        if not self.running:
            yield self.lock.acquire()
            self.lock.release()

    def callWhenRunning(self, f, *args, **kw):
        """
        Calls the I{f-args-kw} combo when the broker is running and ready
        for transactions.
        """
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
            self.running = False
            if self.q.isRunning():
                with self.lock.context() as d:
                    yield d
                    # Calling this via the queue is a problem if the
                    # queue is shared and has been shut down. But
                    # calling it in the main thread seems to work
                    closeConnection()
                    # yield self.q.call(closeConnection)
            yield self.qFactory.kill(self.q)

    @defer.inlineCallbacks
    def handleResult(self, result, consumer=None, conn=None, asList=False, N=1):
        """
        Handles the result of a transaction or connection.execute. If it's
        a C{ResultsProxy} and possibly an implementor of C{IConsumer},
        returned a (deferred) instance of Deferator or couples your
        consumer to an IterationProducer.

        You can supply a connection to be closed after iterations are
        done with the keyword 'conn'. With the keyword I{asList}, you
        can force the rows of a C{ResultProxy} to be fetched (in my
        thread) and returned as a (deferred) list of rows.
        """
        def close(null):
            if callable(getattr(result, 'close', None)):
                result.close()
            if conn:
                conn.close()

        if getattr(result, 'returns_rows', False):
            # A ResultsProxy gets special handling
            if asList:
                # ...except with asList, when all its rows are fetched
                # in my thread and simply returned as a list
                result = yield self.q.deferToThread(result.fetchall)
            else:
                pf = iteration.Prefetcherator(repr(result))
                ok = yield pf.setup(
                    self.q.deferToThread, nextFromRP, result, N)
                if ok:
                    dr = iteration.Deferator(pf)
                    dr.addCallback(close)
                    if consumer:
                        # A consumer was supplied, so try to make an
                        # IterationProducer couple to it.
                        ip = iteration.IterationProducer(dr, consumer)
                        # We "wait" here for the iteration/production
                        # to finish. What actually happens is that the
                        # caller receives a deferred that fires when
                        # iteration/production is done. However, the
                        # consumer gets the iterations in the
                        # meantime.
                        yield ip.run()
                        result = consumer
                    else:
                        # No consumer supplied, just "return" the Deferator
                        result = dr
                else:
                    # Empty/invalid ResultsProxy, just "return" an empty list
                    result = []
        defer.returnValue(result)
            
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
        Just returns an C{SQLAlchemy} select object. You do everything
        else.

        This is an immediate result, not a C{Deferred}.
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
        context of the placeholder::

          with <me>.selex(<table>.c.foo) as sh:
              sh.where(<table>.c.bar == "correct")
          for dRow in sh():
              row = yield dRow
              ...

        You can call this outside of a transaction and get a deferred
        result from calling the placeholder object. For such usage,
        you can specify the usual transaction keywords via keywords to
        this method.
        """
        kw['isNested'] = isNested(self)
        sh = SelectAndResultHolder(self, *args, **kw)
        yield sh
        sh.close()

    @defer.inlineCallbacks
    def selectorator(self, selectObj, consumer=None, de=None, N=1):
        """
        When called with a select object that results in an iterable
        C{ResultProxy} when executed, returns a deferred that fires
        with a C{Deferator} that can be iterated over deferreds, each
        of which fires with a successive row of the select's
        C{ResultProxy}.

        If you supply an C{IConsumer} with the I{consumer} keyword, I
        will couple an C{iteration.IterationProducer} to your consumer
        and run it. The returned C{Deferred} will fire (with a
        reference to your consumer) when the iterations are done, but
        you don't need to wait for that before doing another
        transaction.

        If you supply a C{Deferred} via the I{de} keyword, it will be
        fired (unless already fired for some reason) with C{None} when
        the select object has been executed, but before iterations
        have begun.

        If you want multiple rows produced at once using the
        C{fetchmany} method of the C{ResultProxy}, set I{N} to the
        number of rows you want at a time.
        
        Call directly, *not* from inside a transaction::

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
        if isinstance(de, defer.Deferred) and not de.called:
            de.callback(None)
        result = yield self.handleResult(
            rp, consumer=consumer, conn=connection, N=N)
        defer.returnValue(result)

    @transact
    def execute(self, *args, **kw):
        """
        Does a C{connection.execute(*args, **kw)} as a transaction, in my
        thread with my current connection, with all the usual handling
        of the C{ResultProxy}.
        """
        return self.connection.execute(*args, **kw)

    def sql(self, sqlText, **kw):
        """
        Executes raw SQL as a transaction, in my thread with my current
        connection, with any rows of the result returned as a list.
        """
        kw['asList'] = True
        return self.execute(SA.text(sqlText), **kw)
        
    def deferToQueue(self, func, *args, **kw):
        """
        Dispatches I{callable(*args, **kw)} as a task via the like-named
        method of my asynchronous queue, returning a C{Deferred} to
        its eventual result.

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

