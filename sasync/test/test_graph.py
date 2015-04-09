# sAsync:
# An enhancement to the SQLAlchemy package that provides persistent
# dictionaries, text indexing and searching, and an access broker for
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
Unit tests for graph
"""

import random
from twisted.internet import defer
import networkx as NX

import sasync.graph as graph
from testbase import deferToDelay, TestCase


VERBOSE = False
db = 'graph.db'
URL = "sqlite:///%s" % db


class BaseMixin:
    def cloneGraph(self, G):
        graphName, graphClass = G.name, G.__class__
        H = graphClass(graphName, URL, nodeType=int)
        return H, H.startup()
    
    def newGraph(self, name, diGraph=False, startFresh=False):
        klass = graph.DiGraph if diGraph else graph.Graph
        return klass(name, URL, startFresh=startFresh, nodeType=int)

    def graphGenerator(self, name, startFresh=True):
        """
        Yields dicts containing a persistent (Di)Graph object under
        test. If C{populated}, each dict contains a regular NX.Graph
        object with some data. If C{clear}, the test (Di)Graph object
        is guaranteed to be empty, and of course is not C{populated}.
        """
        for diGraph in (False, True):
            if VERBOSE:
                graphType = 'DiGraph' if diGraph else 'Graph'
                print "\n\nTesting %s ..." % graphType
            G = self.newGraph(
                name, diGraph=diGraph, startFresh=startFresh)
            if VERBOSE:
                edgeStrings = [str(x) for x in G.edges()]
                if edgeStrings:
                    print "\t\t...with edges %s" % '\n'.join(edgeStrings)
                else:
                    print "\t\t...with NO edges"
            yield G, diGraph
        
    def failUnlessSameListElements(self, listA, listB):
        if not isinstance(listA, (list, tuple)):
            listA = [listA]
            listB = [listB]
        listA.sort()
        listB.sort()
        for k, u in enumerate(listA):
            self.failUnlessEqual(
                u, listB[k],
                "Element '%s' in list %s != '%s' in list %s" % \
                (u, listA, listB[k], listB))
    
    def failUnlessGraphsEqual(self, G1, G2):
        args = [G.edges() for G in (G1, G2)]
        for k, graphName in enumerate(('G1', 'G2')):
            if VERBOSE and not args[k]:
                print "\tWARNING: Testing if G1 and G2 " +\
                      "are identical, but %s is empty" % graphName
            args[k].sort()
        args.append(
            "Graphs are not identical: %s vs %s" % (G1.edges(), G2.edges()))
        return self.failUnlessEqual(*args)


class CommonTestsMixin(BaseMixin):
    def tearDown(self):
        return self.G.shutdown()

    def test_addNodes(self):
        def first(null):
            self.H, d = self.cloneGraph(self.G)
            return d

        def second(null):
            nodes = self.H.nodes()
            self.failUnlessSameListElements(nodes, [1, 2])
        
        self.G.add_node(1)
        self.G.add_node(2)
        d = self.G.shutdown()
        d.addCallback(first)
        d.addCallback(second)
        return d

    def test_waitForWrites(self):
        edge = random.sample(xrange(100), 2)
        
        def first(null):
            self.H, d = self.cloneGraph(self.G)
            return d

        def second(null):
            if isinstance(self.G, graph.DiGraph):
                self.failUnlessGraphsEqual(self.G, self.H)
            else:
                nodes = self.H.nodes()
                self.failUnlessSameListElements(nodes, edge)

        self.G.add_edge(*edge)
        d = self.G.deferToWrites()
        d.addCallback(first)
        d.addCallback(second)
        return d


class TestGraphBasics(CommonTestsMixin, TestCase):
    def setUp(self):
        self.G = graph.Graph('foo', URL, nodeType=int)
        return self.G.startup(startFresh=True)

    def test_addEdges(self):
        self.G.add_edge(1, 2)
        self.G.add_edge(2, 3)
        nodes = self.G.nodes()
        self.failUnlessSameListElements(nodes, [1, 2, 3])
        edges = self.G.edges()
        self.failUnlessSameListElements(edges, [(1,2), (2,3)])
    

class TestDiGraphBasics(CommonTestsMixin, TestCase):
    def setUp(self):
        self.G = graph.DiGraph('bar', URL, nodeType=int)
        return self.G.startup(startFresh=True)
    
    def test_addEdges(self):
        self.G.add_edge(1, 2)
        self.G.add_edge(2, 3)
        nodes = self.G.nodes()
        self.failUnlessSameListElements(nodes, [1, 2, 3])
        edges = self.G.edges()
        self.failUnlessSameListElements(edges, [(1,2), (2,3)])


class TestGraphPersistence(BaseMixin, TestCase):
    """
    Do pnetwork.Graph and pnetworkx.DiGraph act like NX.Graph and
    NX.DiGraph items, except for their special persistency features?
    """
    @defer.inlineCallbacks
    def test_persists(self):
        for G1, diGraph in self.graphGenerator('alpha'):
            yield G1.startup()
            G1.add_edge(1, 2)
            yield G1.deferToWrites()
            G2 = self.newGraph('alpha', diGraph=diGraph, startFresh=False)
            yield G2.startup()
            self.failUnlessGraphsEqual(G1, G2)
            yield G1.shutdown()
            yield G2.shutdown()

    @defer.inlineCallbacks
    def test_differentAlistForDifferentNames(self):
        for G1, diGraph in self.graphGenerator('charlie'):
            yield G1.startup()
            G2 = self.newGraph('delta', diGraph=diGraph)
            yield G2.startup()
            self.failIfEqual(id(G1.adj), id(G2.adj))
            yield G1.shutdown()
            yield G2.shutdown()

