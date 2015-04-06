# sAsync:
# An enhancement to the SQLAlchemy package that provides persistent
# dictionaries, text indexing and searching, and an access broker for
# conveniently managing database access, table setup, and
# transactions. Everything can be run in an asynchronous fashion using the
# Twisted framework and its deferred processing capabilities.
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
Persistent Three-dimensional array objects
"""

# Imports
from twisted.internet import defer, reactor
import sqlalchemy as SA

from asynqueue import DeferredTracker
from database import transact, AccessBroker
import search


NICENESS_WRITE = 6


class Transactor(AccessBroker):
    """
    I do the hands-on work of (potentially) non-blocking database access for
    the persistence of array elements within a uniquely-identified group.

    My methods return Twisted deferred instances to the results of their
    database accesses rather than forcing the client code to block while the
    database access is being completed.
    
    """
    def __init__(self, ID, *url, **kw):
        """
        Instantiates me for a three-dimensional array of elements within a
        particular group uniquely identified by the supplied integer I{ID},
        using a database connection to I{url}.
        """
        if not isinstance(ID, int):
            raise TypeError("Item IDs must be integers")
        self.groupID = ID
        self.dt = DeferredTracker()
        super(Transactor, self).__init__(*url[:1], **kw)

    def startup(self):
        """
        You can run my transaction methods when the deferred returned from
        this method fires, and not before.
        """
        return self.table(
            'sasync_array',
            SA.Column('group_id', SA.Integer),
            SA.Column('x', SA.Integer),
            SA.Column('y', SA.Integer),
            SA.Column('z', SA.Integer),
            SA.Column('value', SA.PickleType, nullable=False),
            unique_elements=['group_id', 'x', 'y', 'z']
            )
    
    @transact
    def load(self, x, y, z):
        """
        Element load transaction
        """
        array = self.sasync_array
        if not self.s('load'):
            self.s(
                [array.c.value],
                SA.and_(array.c.group_id == self.groupID,
                        array.c.x == SA.bindparam('x'),
                        array.c.y == SA.bindparam('y'),
                        array.c.z == SA.bindparam('z'))
                )
        rows = self.s().execute(x=hash(x), y=hash(y), z=hash(z)).fetchonly()
        return rows['value'] if rows else None

    @transact
    def update(self, x, y, z, value):
        """
        Element overwrite (entry update) transaction
        """
        elements = self.sasync_array
        u = elements.update(
            SA.and_(elements.c.group_id == self.groupID,
                    elements.c.x == hash(x),
                    elements.c.y == hash(y),
                    elements.c.z == hash(z))
            )
        u.execute(value=value)

    @transact
    def insert(self, x, y, z, value):
        """
        Element add (entry insert) transaction
        """
        self.sasync_array.insert().execute(
            group_id=self.groupID,
            x=hash(x), y=hash(y), z=hash(z), value=value)

    @transact
    def delete(self, x, y, z):
        """
        Element delete transaction
        """
        elements = self.sasync_array
        self.sasync_array.delete(
            SA.and_(elements.c.group_id == self.groupID,
                    elements.c.x == hash(x),
                    elements.c.y == hash(y),
                    elements.c.z == hash(z))
            ).execute()

    @transact
    def clear(self):
        """
        Transaction to clear all elements (B{Use with care!})
        """
        elements = self.sasync_array
        self.sasync_array.delete(
            elements.c.group_id == self.groupID).execute()


class PersistentArray(object):
    """
    I am a three-dimensional array of Python objects, addressable by any
    three-way combination of hashable Python objects. You can use me as a
    two-dimensional array by simply using some constant, e.g., C{None} when
    supplying an address for my third dimension.
    """
    search = None

    def __init__(self, ID, *url, **kw):
        """
        Constructor, with a URL and any engine-specifying keywords
        supplied if a particular engine is to be used for this
        instance. The following additional keyword is particular to
        this constructor:
        
        """
        try:
            self.ID = hash(ID)
        except:
            raise TypeError("Item IDs must be hashable")
        self.t = Transactor(self.ID, *url[:1], **kw)
        self.dt = asynqueue.DeferredTracker()
        reactor.addSystemEventTrigger('before', 'shutdown', self.shutdown)
    
    def shutdown(self, *null):
        """
        Shuts down my database L{Transactor} and its synchronous task queue.
        """
        return self.t.shutdown()

    def write(self, funcName, *args, **kw):
        """
        Performs a database write transaction, returning a deferred to its
        completion.
        """
        func = getattr(self.t, funcName)
        kw = {'niceness':kw.get('niceness', NICENESS_WRITE)}
        return func(*args, **kw)

    def get(self, x, y, z):
        """
        Retrieves an element (x,y,z) from the database.
        """
        d = self.dt.deferToAll()
        d.addCallback(lambda _: self.t.load(x, y, z))
        return d

    def set(self, x, y, z, value):
        """
        Persists the supplied I{value} of element (x,y,z) to the database,
        inserting or updating a row as appropriate.
        """
        def loaded(loadedValue):
            if loadedValue is None:
                return self.write("insert", x, y, z, value)
            return self.write("update", x, y, z, value)
        
        d = self.t.load(x, y, z)
        d.addCallback(loaded)
        self.dt.put(d)
        return d

    def delete(self, x, y, z):
        """
        Deletes the database row for element (x,y,z).
        """
        d = self.write("delete", x, y, z)
        self.dt.put(d)
        return d

    def clear(self):
        """
        Deletes the entire group of database rows for U{all} of my elements
        (B{Use with care!})
        """
        d =self.write("clear", niceness=0)
        self.dt.put(d)
        return d


__all__ = ['PersistentArray']
