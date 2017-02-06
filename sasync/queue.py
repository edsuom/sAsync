# sAsync:
# An enhancement to the SQLAlchemy package that provides persistent
# item-value stores, arrays, and dictionaries, and an access broker for
# conveniently managing database access, table setup, and
# transactions. Everything can be run in an asynchronous fashion using
# the Twisted framework and its deferred processing capabilities.
#
# Copyright (C) 2006, 2015 by Edwin A. Suominen, http://edsuom.com/sAsync
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
Queuing for asynchronous database transactions via C{SQLAlchemy}.

"""

import logging

from twisted.internet import defer

import sqlalchemy as SA
from sqlalchemy import pool

import asynqueue


class Factory(object):
    """
    I generate C{asynqueue.ThreadQueue} objects, a unique one for each
    call to me with a unique url-kw combination.
    """
    globalQueue = None
    
    def __init__(self):
        self.queues = {}

    @staticmethod
    def newQueue(url, **kw):
        """
        Returns a C{Deferred} that fires with a new C{asynqueue.ThreadQueue}
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

        Calling this method, or calling an instance of me without a
        url argument, is the only approved way to get a reference to
        the global queue.
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
        Returns a C{Deferred} that fires with an C{asynqueue.ThreadQueue}
        that has an C{SQLAlchemy} engine attached to it, constructed
        with the supplied url and any keywords. The engine can be
        referenced via the queue's I{engine} attribute.

        If a queue has already been constructed with the same url-kw
        parameters, that same one is returned. Otherwise, a new one is
        constructed and saved for a repeat call.

        If there is no I{url} argument, the global default queue will
        be returned. There must be one for that to work, of course.

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

