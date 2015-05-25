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
Dictionary-like objects with behind-the-scenes database persistence.

B{Caution:} This module has a few test failures and may need some
debugging.
"""

from collections import MutableMapping

from twisted.internet import defer

from asynqueue import DeferredTracker

import items
from errors import AsyncError


class PersistentDictBase(MutableMapping, object):
    """
    I am a base class for a database-persistent dictionary-like object
    uniquely identified by the hashable constructor argument I{ID}.

    Before you use any instance of me, you must specify the parameters
    for creating an SQLAlchemy database engine. A single argument is
    used, which specifies a connection to a database via an RFC-1738
    url. In addition, the following keyword options can be employed,
    which are listed in the API docs for L{sasync} and
    L{database.AccessBroker}.

    You can set an engine globally, for all instances of me, via the
    L{sasync.engine} package-level function, or via the
    L{queue.Factory.setGlobal} class method. Alternatively, you can
    specify an engine for one particular instance by supplying the
    parameters to my constructor.
    
    In my default mode of operation, both read and write item accesses
    occur asynchronously and return deferreds. However, you can put me
    into B{load} mode by calling my L{preload} method. At that point,
    all my items will be accessed synchronously as with any other
    dictionary. No other deferreds will be returned from any item
    access. Lazy writing will still be done, but behind the scenes and
    with no API access to write completions.

    B{IMPORTANT}: As with all C{sAsync} data store objects, make sure
    you call my L{shutdown} method for an instance of me that you're
    done with before allowing that instance to be deleted.

    @ivar isPreloadMode: Boolean flag that indicates if I am operating in
        preload mode.
    """
    def __init__(self, ID, *url, **kw):
        """
        Instantiates me with an item store keyed to the supplied hashable
        I{ID}.

        In addition to any engine-specifying keywords supplied, the following
        are particular to this constructor:

        @param ID: A hashable object that is used as my unique identifier.
        
        """
        try:
            self.ID = hash(ID)
        except:
            raise TypeError("Item IDs must be hashable")
        # In-memory Caches
        self.data, self.keyCache = {}, {}
        # For tracking lazy writes
        self.writeTracker = DeferredTracker()
        # My very own persistent items store
        if url:
            self.i = items.Items(self.ID, url[0], **kw)
        else:
            self.i = items.Items(self.ID)
        self.isPreloadMode = False

    def waitUntilRunning(self):
        return self.i.waitUntilRunning()
        
    def preload(self):
        """
        This method preloads all my items from the database (which may take a
        while), returning a C{Deferred} that fires when everything's ready and
        I've completed the transition into B{preload} mode.
        """
        d = self.loadAll()
        d.addCallback(lambda _: setattr(self, 'isPreloadMode', True))
        return d
    
    def shutdown(self, *null):
        """
        Shuts down my database L{AccessBroker} and its synchronous task
        queue.
        """
        return self.writeTracker.deferToAll().addCallback(
            lambda _: self.i.shutdown())

    @defer.inlineCallbacks
    def loadAll(self, *null):
        """
        Loads all items from the database, setting my in-memory dict and key
        cache accordingly.
        """
        yield self.deferToWrites()
        items = yield self.i.loadAll()
        self.data.clear()
        self.data.update(items)
        self.keyCache = dict.fromkeys(items.keys(), True)
        defer.returnValue(self.data)

    def deferToWrites(self, lastOnly=False):
        """
        @see: C{asynqueue.DeferredTracker}
        """
        if lastOnly:
            return self.writeTracker.deferToLast()
        return self.writeTracker.deferToAll()


class PersistentDict(PersistentDictBase):
    """
    I am a database-persistent dictionary-like object with memory caching of
    items and lazy writing.
    
    Getting, setting, or deleting my items returns C{Deferred} objects
    of the Twisted asynchronous framework that fire when the
    underlying database accesses are completed. Returning a deferred
    value avoids forcing the client code to block while the real value
    is being read from the database.

    @ivar data: The in-memory dictionary that each instance of me uses
      to cache values for a given ID.
    """
    
    #--- Core dict operations -------------------------------------------------
    
    def __getitem__(self, name):
        """
        Returns a C{Deferred} to the value of item I{name} or the value itself
        if in preload mode.

        The value is only loaded from the database if it isn't already in the
        in-memory dictionary.
        """
        def valueLoaded(value):
            if isinstance(value, items.Missing):
                raise KeyError(
                    "No item '%s' in the database" % name)
            self.data[name] = value
            self.keyCache.setdefault(name, False)
            return value

        if name in self.data:
            value = self.data[name]
            if self.isPreloadMode:
                return value
            else:
                return defer.succeed(value)
        elif self.isPreloadMode:
            raise KeyError(
                "No item '%s' in the database" % name)
        else:
            return self.i.load(name).addCallback(valueLoaded)

    def __setitem__(self, name, value):
        """
        Sets item I{name} to I{value}, saving it to the database if there
        isn't already an in-memory dictionary item with that exact value.
        """
        def valueLoaded(loadedValue):
            if isinstance(loadedValue, items.Missing):
                # Item isn't in the database, so insert it
                d = self.i.insert(name, value)
            else:
                # Update current value of item in the database
                d = self.i.update(name, value)
            d.addCallback(done)
            return d
        def done(x):
            d2.callback(None)
            return x

        oldValue = self.data.get(name, None)
        self.data[name] = value
        self.keyCache.setdefault(name, False)
        # Everything from here on is just lazy writing
        if oldValue is None:
            # We're writing an item that hasn't been loaded from the database
            # yet
            if self.isPreloadMode:
                # If it hasn't been loaded yet, in preload mode, it ain't there
                d = self.i.insert(name, value)
            else:
                # Not in preload mode, so it may be in the database
                # but not yet loaded. Note that we need to also track
                # a Deferred for whichever transaction is done next.
                d2 = defer.Deferred()
                self.writeTracker.put(d2)
                d = self.i.load(name).addCallback(valueLoaded)
        else:
            # There's already a value in the in-memory dictionary, update it
            d = self.i.update(name, value)
        self.writeTracker.put(d)

    def __delitem__(self, name):
        """
        Deletes item I{name}, removing its entry from both the in-memory
        dictionary and the database
        """
        if name in self.data:
            del self.data[name]
            self.keyCache.pop(name, None)
            d = self.i.delete(name)
            self.writeTracker.put(d)
        else:
            raise KeyError(name)

    def __contains__(self, key):
        """
        Indicates if I contain item I{key}.

        In I{preload} mode, returns C{True} if the item is present in my
        in-memory dictionary and C{False} if not.

        In normal mode, returns an immediate C{Deferred} firing with C{True}
        without a transaction if the item is already present in my in-memory
        dictionary. If it isn't, tries to load the item (it will probably be
        requested soon anyhow) and returns a C{Deferred} that will ultimately
        fire with C{True} unless the load resulted in a L{Missing} object. In
        that case, deletes the loaded C{Missing} object from my in-memory
        dictionary and fires the deferred with C{False}.

        Using the C{<key> in <dict>} Python construct doesn't seem to work in
        normal mode. Use L{has_key} instead.
        """
        if self.isPreloadMode:
            return self.data.__contains__(key)
        elif key in self.data or key in self.keyCache:
            return defer.succeed(True)
        else:
            d = self.i.load(key)
            d.addCallback(lambda value: not isinstance(value, items.Missing))
            return d

    def keys(self):
        """
        Returns an immediate or deferred list of the names of all my items in
        the database.
        """
        def gotKeyList(keyList):
            self.keyCache = dict.fromkeys(keyList, True)
            return keyList

        if self.isPreloadMode:
            return self.data.keys()
        if True in self.keyCache.values():
            # The key cache is valid as long as it has entries (=True)
            # that were retrieved from preloading or a previous call
            # of this method. The __setitem__ method will add new keys
            # to the cache, but that doesn't initialize it.
            keys = self.keyCache.keys()
            return defer.succeed(keys)
        # Empty or invalid key cache, load and cache a list of keys
        return self.i.names().addCallback(gotKeyList)

    #--- Replacement dict methods as needed -----------------------------------

    def has_key(self, key):
        """
        Returns an immediate or deferred Boolean indicating whether the key is
        present.
        """
        return self.__contains__(key)

    def clear(self):
        """
        Clears the in-memory dictionary of all items and deletes all their
        database entries.
        """
        self.keyCache.clear()
        self.data.clear()
        d = self.writeTracker.deferToAll()
        d.addCallback(lambda _: self.i.names())
        d.addCallback(lambda names: self.i.delete(*names))
        self.writeTracker.put(d)
        return d

    def __iter__(self):
        """
        B{Only for preload mode}: Iterate over all my keys.
        """
        return iter(self.iterkeys)

    def iteritems(self):
        """
        B{Only for preload mode}: Iterate over all my items.
        """
        if self.isPreloadMode:
            for item in self.data.iteritems():
                yield item
        else:
            raise AsyncError("Can't iterate asynchronously")

    def iterkeys(self):
        """
        B{Only for preload mode}: Iterate over all my keys.
        """
        if self.isPreloadMode:
            for key in self.data.iterkeys():
                yield key
        else:
            raise AsyncError("Can't iterate asynchronously")

    def itervalues(self):
        """
        B{Only for preload mode}: Iterate over all my values.
        """
        if self.isPreloadMode:
            for value in self.data.itervalues():
                yield values
        else:
            raise AsyncError("Can't iterate asynchronously")

    def __len__(self):
        """
        Returns an immediate or deferred integer with my length, i.e.,
        the number of keys.
        """
        if self.isPreloadMode:
            return len(self.data)
        return self.loadAll().addCallback(lambda x: len(x))

    def items(self):
        """
        Returns an immediate or deferred sequence of (name, value) tuples
        representing all my items.
        """
        if self.isPreloadMode:
            return self.data.items()
        return self.loadAll().addCallback(lambda x: x.items())
            
    def values(self):
        """
        Returns an immediate or deferred sequence of all my values.
        """
        if self.isPreloadMode:
            return self.data.values()
        return self.loadAll().addCallback(lambda x: x.values())

    def get(self, *args):
        """
        Returns an immediate or deferred value of the value for the key
        specified as the first argument, or a default value if specified as an
        optional second argument. If the item is not present and no default
        value is supplied, raises the appropriate exception.
        """
        def gotItem(loadedValue, key, defaultValue):
            if isinstance(loadedValue, items.Missing):
                return defaultValue
            self.data[key] = loadedValue
            return loadedValue

        key = args[0]
        if len(args) == 1:
            return self[key]
        defaultValue = args[1]
        if self.isPreloadMode:
            if self.has_key(key):
                return self[key]
            return defaultValue
        d = self.i.load(key)
        d.addCallback(gotItem, key, defaultValue)
        return d

    def setdefault(self, key, value):
        """
        Sets my item specified by I{key} to I{value} if it doesn't exist
        already.  Returns an immediate or deferred reference to the item's
        value after its new value (if any) is set.
        """
        def gotItem(loadedValue):
            if isinstance(loadedValue, items.Missing):
                self.__setitem__(key, value)
                d = self.writeTracker.deferToLast()
                d.addCallback(lambda _: value)
                return d
            self.data[key] = loadedValue
            return loadedValue

        if self.isPreloadMode:
            if key in self.data:
                return self.data[key]
            self.__setitem__(key, value)
            return value
        if key in self.data:
            return defer.succeed(self.data[key])
        return self.i.load(key).addCallback(gotItem)
    
    def copy(self):
        """
        Returns an immediate or deferred copy of me that is a conventional
        (non-persisted) dictionary.
        """
        if self.isPreloadMode:
            return self.data.copy()
        return self.loadAll()


            
    
        

