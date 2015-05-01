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
Transparently persistent versions of the four different
I{NetworkX} graph objects: C{Graph}, C{DiGraph}, C{XGraph}, and
C{XDiGraph}.

B{Note:} This module has issues. Directed graphs don't seem to work
anymore.


Module Overview
===============

Provides transparently persistent versions of the four different I{NetworkX}
graph objects: C{Graph}, C{DiGraph}, C{XGraph}, and C{XDiGraph}.

The persistent versions of those objects work just like the regular ones from
the I{NetworkX} package except that they are instantiated with an active
SQLAlchemy C{engine} object. Then the objects' contents are read from and
stored to the database with which that object was created, without any fuss on
the user's part.


Usage
=====

This module, like the rest of sAsync, requires the use of the Twisted
asynchronous processing framework. Twisted is very powerful, but its
event-driven, non-blocking way of life takes some getting used to.

Otherwise, usage is very simple, changing just a line or two of your regular
L{networkx} code::

  from sasync.graph import Graph
  
  ...

  # Instantiate a persistent graph object with its own name (a required
  # argument for filing the persisted data, not just a keyword option)
  G = Graph('foo')
  
  # Start up its persistence engine and return a Deferred object that fires
  # when it's ready
  d = G.startup('sqlite:///graph.db')
  
  # Add a callback to the Deferred to proceed with using the graph object
  # somehow
  d.addCallback(...)

  ...

When the startup C{Deferred} fires, the instantiated graph will have
whatever node and edge values you set it to last time you instantiated
it.
"""

from twisted.internet import defer
import networkx as NX
from pdict import PersistentDict


class Persistent(object):
    """
    I am a mixin used in persistent subclasses of NetworkX C{Graph}
    classes for both directed and undirected graphs, with or without
    edge weights. I rely on a persistent dictionary-like-object from
    the I{sAsync} package to achieve my behind-the-scenes persistency.

    My persistent contents are common to all instances of me having
    the same graph name, which can be defined as an option and
    defaults to 'No Name'. For example, a second instance of me
    invoked with no keywords will be instantiated with the same graph
    contents as a first one, but C{Graph(name='foo')} will be a
    different graph. If you want different instances of C{Graph},
    instantiate them with different I{name} options.
    """
    @defer.inlineCallbacks
    def startup(self, startFresh=None):
        """
        Starts up my persistence engine.

        This method replaces the stock C{adj} adjacency list attribute
        with an instance of L{PersistentDict}, or for directed graphs,
        by replacing the C{succ} and C{pred} adjacency list attributes
        with two such instances.

        Important note regarding directed graph persistence
        ===================================================

        In directed graphs, we actually overwrite the C{adj} attribute
        instead of C{succ}. That's because C{adj} is used in the
        C{NX.Graph} superclass and C{succ} is merely set equal to it
        in the constructor of C{NX.DiGraph}.

        When C{x = y} in Python, changing some property of C{y} causes
        the same change in C{x}. However, we are changing the
        attribute C{adj} to reference an I{entirely new} object, not
        just changing its properties. Thus we have to refresh the
        C{self.succ = self.adj} link after overwriting C{adj}, giving
        C{succ} a reference to the I{new} C{adj} object. Adding or
        changing I{items} of either one will show up in the other, so
        no further hacking is required.

        @param startFresh: Set this keyword C{True} to clear any
          persisted content and start fresh. This keyword can also be
          set in the constructor. Obviously, you should use this
          option with care as it will B{erase} database entries!

        @return: A deferred that fires when the persistence engine is
          ready for use.
        """
        def ID():
            self._uniqueCount += 1
            thisID = "%s-%d" % (self.name, self._uniqueCount)
            return hash(thisID)

        if startFresh is None:
            startFresh = getattr(self, 'startFresh', False)
        self._uniqueCount = 0
        self.kw['nameType'] = self.nodeType
        # Adjacency lists
        for dictName in self.adjacencyLists:
            dictObject = PersistentDict(ID(), self.url, **self.kw)
            setattr(self, dictName, dictObject)
            yield dictObject.preload()
        self.succ = self.adj
        if startFresh or self.startFresh:
            yield self.adjacencyListOperation("clear")
    
    def shutdown(self):
        """
        Shuts down my persistence engine.

        @return: A deferred that fires when the persistence engine is
          shut down and I can safely be deleted.
        
        """
        return self.adjacencyListOperation("shutdown")

    def deferToWrites(self):
        """
        Returns a deferred that fires when all outstanding lazy writes are
        done. You should only need this if you are going to be doing
        something with the underlying database.
        """
        return self.adjacencyListOperation("deferToWrites")
    
    def adjacencyListOperation(self, methodName):
        """
        Private method that performs the named I{method} of all my
        persistent adjacency list dictionary or dictionaries.

        Returns a deferredList that fires when the deferreds resulting
        from each method call have fired.
        """
        dList = []
        for dictName in self.adjacencyLists:
            dictObject = getattr(self, dictName, None)
            methodObject = getattr(dictObject, methodName, None)
            if callable(methodObject):
                dList.append(methodObject())
        return defer.DeferredList(dList)


class Graph(Persistent, NX.Graph):
    """
    Persistent version of L{NX.Graph}
    """
    adjacencyLists = ('adj',)
    def __init__(self, name, url, **kw):
        self.nodeType = kw.pop('nodeType', str)
        self.startFresh = kw.pop('startFresh', False)
        NX.Graph.__init__(self, name=name)
        self.url = url
        self.kw = kw

        
class DiGraph(Persistent, NX.DiGraph):
    """
    Persistent version of L{NX.DiGraph}
    """
    adjacencyLists = ('adj', 'pred')
    def __init__(self, name, url, **kw):
        self.nodeType = kw.pop('nodeType', str)
        self.startFresh = kw.pop('startFresh', False)
        NX.DiGraph.__init__(self, name=name)
        self.url = url
        self.kw = kw


class MultiGraph(Persistent, NX.MultiGraph):
    """
    Persistent version of L{NX.MultiGraph}
    """
    adjacencyLists = ('adj',)
    def __init__(self, name, url, **kw):
        self.nodeType = kw.pop('nodeType', str)
        self.startFresh = kw.pop('startFresh', False)
        NX.MultiGraph.__init__(self, name=name)
        self.url = url
        self.kw = kw


class MultiDiGraph(Persistent, NX.MultiDiGraph):
    """
    Persistent version of L{NX.MultiDiGraph}
    """
    adjacencyLists = ('adj', 'pred')
    def __init__(self, name, url, **kw):
        self.nodeType = kw.pop('nodeType', str)
        self.startFresh = kw.pop('startFresh', False)
        NX.MultiDiGraph.__init__(self, name=name)
        self.url = url
        self.kw = kw
