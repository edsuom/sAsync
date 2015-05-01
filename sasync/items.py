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
Dictionary-like objects with behind-the-scenes database persistence.

L{Items} provides a public interface for non-blocking database access
to persistently stored name:value items within a uniquely-identified
group, e.g., for a persistent dictionary using L{pdict.PersistentDict}.
"""

# Imports
from twisted.internet import defer
import sqlalchemy as SA

from database import transact, AccessBroker


NICENESS_WRITE = 6


class Missing:
    """
    An instance of me is returned as the value of a missing item.
    """
    def __init__(self, group, name):
        self.group, self.name = group, name


class Transactor(AccessBroker):
    """
    I do the hands-on work of non-blocking database access for the
    persistence of C{name:value} items within a uniquely-identified
    group, e.g., for a persistent dictionary using
    L{pdict.PersistentDict}.

    My methods return Twisted C{Deferred} instances to the results of
    their database accesses rather than forcing the client code to
    block while the database access is being completed.
    """
    def __init__(self, ID, *url, **kw):
        """
        Instantiates me for the items of a particular group uniquely identified
        by the supplied integer I{ID}, optionally using a particular database
        connection to I{url} with any supplied keywords.
        """
        if not isinstance(ID, int):
            raise TypeError("Item IDs must be integers")
        self.groupID = ID
        if url:
            AccessBroker.__init__(self, url[0], **kw)
        else:
            AccessBroker.__init__(self)
    
    def startup(self):
        """
        Startup method, automatically called before the first transaction.
        """
        return self.table(
            'sasync_items',
            SA.Column('group_id', SA.Integer, primary_key=True),
            SA.Column('name', SA.String(40), primary_key=True),
            SA.Column('value', SA.PickleType, nullable=False)
            )
    
    @transact
    def load(self, name):
        """
        Item load transaction
        """
        items = self.sasync_items
        if not self.s('load'):
            self.s(
                [items.c.value],
                SA.and_(items.c.group_id == self.groupID,
                        items.c.name == SA.bindparam('name')))
        row = self.s().execute(name=name).fetchone()
        if not row:
            return Missing(self.groupID, name)
        else:
            return row['value']
    
    @transact
    def loadAll(self):
        """
        Load all my items, returing a name:value dict
        """
        items = self.sasync_items
        if not self.s('load_all'):
            self.s(
                [items.c.name, items.c.value],
                items.c.group_id == self.groupID)
        rows = self.s().execute().fetchall()
        result = {}
        for row in rows:
            result[row['name']] = row['value']
        return result

    @transact
    def update(self, name, value):
        """
        Item overwrite (entry update) transaction
        """
        items = self.sasync_items
        u = items.update(
            SA.and_(items.c.group_id == self.groupID,
                    items.c.name == name))
        u.execute(value=value)
    
    @transact
    def insert(self, name, value):
        """
        Item add (entry insert) transaction
        """
        self.sasync_items.insert().execute(
            group_id=self.groupID, name=name, value=value)

    @transact
    def delete(self, *names):
        """
        Item(s) delete transaction
        """
        items = self.sasync_items
        self.sasync_items.delete(
            SA.and_(items.c.group_id == self.groupID,
                    items.c.name.in_(names))).execute()
    
    @transact
    def names(self):
        """
        All item names loading transaction
        """
        items = self.sasync_items
        if not self.s('names'):
            self.s(
                [items.c.name],
                items.c.group_id == self.groupID)
        return [str(x[0]) for x in self.s().execute().fetchall()]


class Items(object):
    """
    I provide a public interface for non-blocking database access to
    persistently stored name:value items within a uniquely-identified group,
    e.g., for a persistent dictionary using L{pdict.PersistentDict}.

    Before you use any instance of me, you must specify the parameters for
    creating an SQLAlchemy database engine. A single argument is used, which
    specifies a connection to a database via an RFC-1738 url. In addition, the
    following keyword options can be employed, which are listed in the API docs
    for L{sasync} and L{database.AccessBroker}.

    You can set an engine globally, for all instances of me via the
    L{sasync.engine} package-level function, or via the L{AccessBroker.engine}
    class method. Alternatively, you can specify an engine for one particular
    instance by supplying the parameters to my constructor.
    """
    def __init__(self, ID, *url, **kw):
        """
        Instantiates me for the items of a particular group uniquely
        identified by the supplied hashable I{ID}.

        In addition to any engine-specifying keywords supplied, the following
        are particular to this constructor:

        @param ID: A hashable object that is used as my unique identifier.

        @keyword nameType: A C{type} object defining the type that each name
            will be coerced to after being loaded as a string from the
            database.
        """
        try:
            self.groupID = hash(ID)
        except:
            raise TypeError("Item IDs must be hashable")
        self.nameType = kw.pop('nameType', str)
        if url:
            self.t = Transactor(self.groupID, url[0], **kw)
        else:
            self.t = Transactor(self.groupID)
        for name in ('waitUntilRunning', 'callWhenRunning', 'shutdown'):
            setattr(self, name, getattr(self.t, name))

    def write(self, funcName, name, value, niceness=None):
        """
        Performs a database write transaction, returning a deferred to its
        completion.
        """
        func = getattr(self.t, funcName)
        if niceness is None:
            niceness = NICENESS_WRITE
        return self.callWhenRunning(func, name, value, niceness=niceness)
    
    def load(self, name):
        """
        Loads item I{name} from the database, returning a deferred to the
        loaded value. A L{Missing} object represents the value of a missing
        item.
        """
        return self.callWhenRunning(self.t.load, name)

    @defer.inlineCallbacks
    def loadAll(self):
        """
        Loads all items in my group from the database, returning a
        deferred to a dict of the loaded values. The keys of the dict
        are coerced to the type of my I{nameType} attribute.
        """
        newDict = {}
        yield self.waitUntilRunning()
        valueDict = yield self.t.loadAll()
        for name, value in valueDict.iteritems():
            key = self.nameType(name)
            newDict[key] = value
        defer.returnValue(newDict)
    
    def update(self, name, value):
        """
        Updates the database entry for item I{name} = I{value}, returning a
        deferred that fires when the transaction is done.
        """
        return self.write('update', name, value)
    
    def insert(self, name, value):
        """
        Inserts a database entry for item I{name} = I{value}, returning a
        deferred that fires when the transaction is done.
        """
        return self.write('insert', name, value)
    
    def delete(self, *names):
        """
        Deletes the database entries for the items having the supplied
        I{*names}, returning a deferred that fires when the transaction is
        done.
        """
        return self.t.delete(*names)
    
    def names(self):
        """
        Returns a deferred that fires with a list of the names of all items
        currently defined in my group.
        """
        def gotNames(names):
            return [self.nameType(x) for x in names]
        
        d = self.t.names()
        d.addCallback(gotNames)
        return d


__all__ = ['Missing', 'Items']
