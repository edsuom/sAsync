# sAsync:
# An enhancement to the SQLAlchemy package that provides persistent
# dictionaries, text indexing and searching, and an access broker for
# conveniently managing database access, table setup, and
# transactions. Everything is run in an asynchronous fashion using the Twisted
# framework and its deferred processing capabilities.
#
# Copyright (C) 2006-2007 by Edwin A. Suominen, http://www.eepatents.com
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
Asynchronous database transactions via SQLAlchemy.
"""

import sys, logging
from contextlib import contextmanager

from twisted.internet import defer
from twisted.python import failure

import sqlalchemy as SA
from sqlalchemy import pool

import asynqueue
from asynqueue import iteration

import select, queue


def nextFromRP(rp):
    print "NFR"
    try:
        row = rp.fetchone()
    except:
        row = None
    if row:
        print "ROW", row
        return row
    print "SI"
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

        If the transaction resulted in a valid iterator and a legit
        consumer was supplied via the consumer keyword, the result is
        an IterationProduced and what is returned is a deferred that
        fires when all iterations have been produced. Because the rows
        have been loaded into memory at that point, callers need not
        actually wait for the deferred to fire before doing another
        transaction.

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
            except Exception, e:
                trans.rollback()
                if ignore:
                    return
                return failure.Failure(e)
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
                    
        ignore = kw.pop('ignore', False)
        consumer = kw.pop('consumer', None)
        if isNested():
            # The call and its result only get special treatment in
            # the outermost @transact function
            result = f(self, *args, **kw)
        else:
            # Here's where the ThreadQueue actually runs the
            # transaction
            if not self.running:
                # Not yet running, "wait" here for queue, engine, and
                # connection
                yield self.lock.acquire()
                # We don't want to hold onto the lock because
                # transactions are queued in the ThreadQueue
                self.lock.release()
            print "\nA", f, args, kw
            result = yield self.q.call(transaction, f, *args, **kw)
            if getattr(result, 'returns_rows', False):
                # A ResultsProxy...                
                pf = iteration.Prefetcherator(repr(result))
                ok = yield pf.setup(self.q.deferToThread, nextFromRP, result)
                if not ok:
                    # Prefetcherator wouldn't accept it (will this
                    # ever happen)?
                    if not ignore:
                        # ...and are not ignoring the error
                        result = failure.Failure(Exception(
                            "Prefetcherator rejected results proxy!"))
                dr = iteration.Deferator(pf)
                # ...that produced a fine, capable Deferator
                if consumer:
                    # ...with a consumer supplied, so try to make an
                    # IterationProducer couple to it
                    ip = iteration.IterationProducer(dr, consumer)
                    yield ip.run()
                    result = None
                else:
                    # ...with no consumer supplied, just return the Deferator
                    result = dr
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

    @ivar q: A property-generated reference to a threaded task queue that is
      dedicated to my database connection.

    @ivar connection: The current SQLAlchemy connection object, if
      any yet exists. Generated by my L{connect} method.
    """
    qFactory = queue.Factory()

    @classmethod
    def setup(cls, url, **kw):
        """
        Constructs a global queue for all instances of me, returning a
        deferred that fires with it.
        """
        return cls.qFactory(True, url, **kw)
    
    def __init__(self, *args, **kw):
        """
        Constructs an instance of me, optionally specifying parameters for
        an SQLAlchemy engine object that serves this instance only.
        """
        @defer.inlineCallbacks
        def startup(null):
            url = args[0] if args else None
            # Queue with attached engine, possibly shared with other
            # AccessBrokers
            self.q = yield self.qFactory(False, url, **kw)
            # A connection of my very own
            self.connection = yield self.connect()
            # Pre-transaction startup, called in main loop after
            # connection made.
            yield defer.maybeDeferred(self.startup)
            # First transaction, called via the queue
            yield transact(self.first)
            # Ready for regular transactions
            self.running = True
            self.lock.release()
        
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

        if hasattr(self, name):
            return defer.succeed(None)
        return self.q.deferToThread(makeTable).addCallback(
            lambda x: self.q.deferToThread(makeIndex, x))
    
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
    def shutdown(self):
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
        sh = select.SelectAndResultHolder(self.connection, *args)
        yield sh
        sh.close()

    def selectorator(self, selectObj):
        """
        When called with a select object that results in an iterable
        ResultProxy when executed, returns a a Deferator that can be
        iterated over deferreds, each of which fires with a successive
        row of the select's ResultProxy.

        Call directly, *not* from inside a transaction.

        # TODO: Finish & test. Need to generalize some of the transact
        stuff so this method call can use it, too.
        """
        rp = self.q.call(self.connection.execute, selectObj)
        pf = asynqueue.Prefetcherator(repr(rp))
        pf.setup(self.q.deferToThread, nextFromRP, rp)
        return asynqueue.Defetcherator(pf)

    def deferToQueue(self, func, *args, **kw):
        """
        Dispatches I{callable(*args, **kw)} as a task via the like-named method
        of my asynchronous queue, returning a deferred to its eventual result.

        Scheduling of the task is impacted by the I{niceness} keyword that can
        be included in I{**kw}. As with UNIX niceness, the value should be an
        integer where 0 is normal scheduling, negative numbers are higher
        priority, and positive numbers are lower priority.
        
        @keyword niceness: Scheduling niceness, an integer between -20 and 20,
            with lower numbers having higher scheduling priority as in UNIX
            C{nice} and C{renice}.
        
        """
        return self.q.call(func, *args, **kw)


__all__ = ['transact', 'AccessBroker', 'SA']

