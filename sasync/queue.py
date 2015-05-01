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
Queuing for asynchronous database transactions via C{SQLAlchemy}.

"""

import logging

from twisted.internet import defer

import asynqueue

import sqlalchemy as SA
from sqlalchemy import pool


class Factory(object):
    """
    I generate L{asynqueue.ThreadQueue} objects, a unique one for each
    call to me with a unique url-kw combination.
    """
    globalQueue = None
    
    def __init__(self):
        self.queues = {}

    @staticmethod
    def newQueue(url, **kw):
        """
        Returns a C{Deferred} that fires with a new L{asynqueue.ThreadQueue}
        that has a new SQLAlchemy engine attached as its I{engine}
        attribute.
        """
        def getEngine():
            # Add a NullHandler to avoid "No handlers could be
            # found for logger sqlalchemy.pool." messages
            logging.getLogger(
                "sqlalchemy.pool").addHandler(logging.NullHandler())
            # Now create the engine
            return SA.create_engine(url, **kw)
        def gotEngine(engine):
            q.engine = engine
            return q
        # Iterators are always returned as raw because ResultProxy
        # objects are iterators but sAsync is smarter at handling them
        # than AsynQueue.
        q = asynqueue.ThreadQueue(
            raw=True,
            verbose=kw.pop('verbose', False),
            spew=kw.pop('spew', False),
            returnFailure=kw.pop('returnFailure', False))
        return q.call(getEngine).addCallback(gotEngine)
        
    @classmethod
    def getGlobal(cls):
        """
        Returns a deferred reference to the global queue, assuming one has
        been defined with L{setGlobal}.

        Calling this method, or L{get} without a url argument, is the
        only approved way to get a reference to the global queue.
        """
        if cls.globalQueue:
            return defer.succeed(cls.globalQueue)
        d = defer.Deferred()
        if hasattr(cls, 'd') and not cls.d.called:
            cls.d.chainDeferred(d)
        else:
            cls.d = d
        return d
        
    @classmethod
    def setGlobal(cls, url, **kw):
        """
        Sets up a global queue and engine, storing as the default and
        returning a deferred reference to it.

        Calling this method is the only approved way to set the global
        queue.
        """
        def gotQueue(q):
            del cls.d
            cls.globalQueue = q
            return q
        cls.d = cls.newQueue(url, **kw).addCallback(gotQueue)
        return cls.d
        
    def kill(self, q):
        """
        Removes the supplied queue object from my local queue cache and
        shuts down the queue. Returns a C{Deferred} that fires when
        the removal and shutdown are done.

        Has no effect on the global queue.
        """
        for key, value in self.queues.iteritems():
            if value == q:
                # Found it. Delete and quit looking.
                del self.queues[key]
                break
        if q == self.globalQueue:
            # We can't kill the global queue
            return defer.succeed(None)
        # Shut 'er down
        return q.shutdown()

    def __call__(self, *url, **kw):
        """
        Returns a C{Deferred} that fires with an L{asynqueue.ThreadQueue}
        that has an C{SQLAlchemy} engine attached to it, constructed
        with the supplied url and any keywords. The engine can be
        referenced via the queue's I{engine} attribute.

        If a queue has already been constructed with the same url-kw
        parameters, that same one is returned. Otherwise, a new one is
        constructed and saved for a repeat call.

        If there is no url argument, the global default queue will be
        returned. There must be one for that to work, of course.

        Separate instances of me can have separate queues for the
        exact same url-kw parameters. But all instances share the same
        global queue.
        """
        def gotQueue(q):
            self.queues[key] = q
            return q

        if url:
            url = url[0]
            key = hash((url, tuple(kw.items())))
            if key in self.queues:
                return defer.succeed(self.queues[key])
            return self.newQueue(url, **kw).addCallback(gotQueue)
        return self.getGlobal()

