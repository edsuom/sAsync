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

import logging

import asynqueue

import sqlalchemy as SA
from sqlalchemy import pool

class Factory(object):
    """
    I generate ThreadQueue objects, a unique one for each call to me
    with a unique url-kw combination.
    """
    def __init__(self):
        self.queues = {}
    
    def key(self, url, kw):
        return hash((url, tuple(kw.items())))
    
    def __call__(self, isGlobal, url, **kw):
        """
        Returns a deferred that fires with an Asynqueue ThreadQueue that
        has an SQLAlchemy engine attached to it, constructed with the
        supplied url and any keywords. The engine can be referenced
        via the queue's 'engine' attribute.

        If a queue has already been constructed with the same url-kw
        parameters, that same one is returned. Otherwise, a new one is
        constructed and saved for a repeat call.

        If the url is C{None}, the global default queue will be
        returned. There must be one for that to work, of course.
        """
        def makeEngine():
            # Add a NullHandler to avoid "No handlers could be
            # found for logger sqlalchemy.pool." messages
            logging.getLogger(
                "sqlalchemy.pool").addHandler(logging.NullHandler())
            # Now create the engine
            # It might be nice to allow multiple threads, but need to
            # consider how that would impact transactions that depend
            # on something being set up.
            kw['pool_size'] = 1
            kw['poolclass'] = pool.SingletonThreadPool
            return SA.create_engine(url, **kw)

        def gotEngine(engine):
            q.engine = engine
            self.queues[key] = q
            if isGlobal:
                # For global, save a default reference, too
                self.queues[None] = q

        if isGlobal and not url:
            raise ValueError("Must specify a url to set a global default")
        if url:
            key = self.key(url, kw)
        elif None in self.queues:
            key = None
        else:
            raise KeyError("No global queue defined")
        if key in self.queues:
            return defer.succeed(self.queues[key])
        q = asynqueue.ThreadQueue(raw=True)
        return q.call(makeEngine).addCallback(gotEngine)
