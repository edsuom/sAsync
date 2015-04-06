# Twisted Goodies:
# Miscellaneous add-ons and improvements to the separately maintained and
# licensed Twisted (TM) asynchronous framework. Permission to use the name was
# graciously granted by Twisted Matrix Laboratories, http://twistedmatrix.com.
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
Intelligent import, Mock objects, and an improved TestCase for AsynQueue
"""

import re, sys, os.path, time, random

from zope.interface import implements
from twisted.internet import reactor, defer
from twisted.internet.interfaces import IConsumer

from twisted.trial import unittest


VERBOSE = False


def deferToDelay(delay):
    d = defer.Deferred()
    reactor.callLater(delay, d.callback, None)
    return d


class MsgBase(object):
    """
    A mixin for providing a convenient message method.
    """
    def msg(self, proto, *args):
        if hasattr(self, 'verbose'):
            verbose = self.verbose
        elif 'VERBOSE' in globals():
            verbose = VERBOSE
        else:
            verbose = False
        if verbose:
            if not hasattr(self, 'msgAlready'):
                proto = "\n" + proto
                self.msgAlready = True
            if args and args[-1] == "-":
                args = args[:-1]
                proto += "\n{}".format("-"*40)
            print proto.format(*args)


class MockTransaction(MsgBase):
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.active = True
        self.msg("Transaction begun")
    def commit(self):
        self.msg("Transaction committed")
        self.active = False
    def rollback(self):
        self.msg("Transaction rolled back")
        self.active = False

class MockConnection(object):
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.msg("Connection open")
    def begin(self):
        return MockTransaction()

class MockQueue(object):
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.lock = asynqueue.DeferredLock()
    @defer.inlineCallbacks
    def call(f, *args, **kw):
        yield self.lock.acquire()
        result = f(*args, **kw)
        self.msg("Ran function {}", repr(f))
        self.lock.release()
        defer.returnValue(result)
        
class MockBroker(object):
    def __init__(self, verbose=False, delay=0.1):
        def running(null):
            self.running = True
            self.msg("Access Broker running")
            self.lock.release()
        self.verbose = verbose
        self.lock = asynqueue.DeferredLock()
        self.lock.acquire()
        return deferToDelay(delay).addCallback(running)
            

class IterationConsumer(MsgBase):
    implements(IConsumer)

    def __init__(self, verbose=False, writeTime=None):
        self.verbose = verbose
        self.writeTime = writeTime
        self.producer = None

    def registerProducer(self, producer, streaming):
        if self.producer:
            raise RuntimeError()
        self.producer = producer
        producer.registerConsumer(self)
        self.data = []
        self.msg(
            "Registered with producer {}. Streaming: {}",
            repr(producer), repr(streaming))

    def unregisterProducer(self):
        self.producer = None
        self.msg("Producer unregistered")

    def write(self, data):
        def resume(null):
            if self.producer:
                self.producer.resumeProducing()
        
        self.data.append(data)
        self.msg("Data received: '{}'", str(data))
        if self.writeTime:
            self.producer.pauseProducing()
            self.d = deferToDelay(
                self.writeTime).addCallback(resume)


class TestCase(MsgBase, unittest.TestCase):
    """
    Slightly improved TestCase
    """
    def doCleanups(self):
        if hasattr(self, 'msgAlready'):
            del self.msgAlready
        return super(TestCase, self).doCleanups()

    def multiplerator(self, N, expected):
        def check(null):
            self.assertEqual(resultList, expected)
            del self.d
        
        dList = []
        resultList = []
        for k in xrange(N):
            yield k
            self.d.addCallback(resultList.append)
            dList.append(self.d)
        self.dm = defer.DeferredList(dList).addCallback(check)
            
    def checkOccurrences(self, pattern, text, number):
        occurrences = len(re.findall(pattern, text))
        if occurrences != number:
            info = \
                u"Expected {:d} occurrences, not {:d}, " +\
                u"of '{}' in\n-----\n{}\n-----\n"
            info = info.format(number, occurrences, pattern, text)
            self.assertEqual(occurrences, number, info)
    
    def checkBegins(self, pattern, text):
        pattern = r"^\s*%s" % (pattern,)
        self.assertTrue(bool(re.match(pattern, text)))

    def checkProducesFile(self, fileName, executable, *args, **kw):
        producedFile = fileInModuleDir(fileName)
        if os.path.exists(producedFile):
            os.remove(producedFile)
        result = executable(*args, **kw)
        self.assertTrue(
            os.path.exists(producedFile),
            "No file '{}' was produced.".format(
                producedFile))
        os.remove(producedFile)
        return result

    def runerator(self, executable, *args, **kw):
        return Runerator(self, executable, *args, **kw)

    def assertPattern(self, pattern, text):
        proto = "Pattern '{}' not in '{}'"
        if '\n' not in pattern:
            text = re.sub(r'\s*\n\s*', '', text)
        if isinstance(text, unicode):
            # What a pain unicode is...
            proto = unicode(proto)
        self.assertTrue(
            bool(re.search(pattern, text)),
            proto.format(pattern, text))

    def assertStringsEqual(self, a, b, msg=""):
        N_seg = 20
        def segment(x):
            k0 = max([0, k-N_seg])
            k1 = min([k+N_seg, len(x)])
            return "{}-!{}!-{}".format(x[k0:k], x[k], x[k+1:k1])
        
        for k, char in enumerate(a):
            if char != b[k]:
                s1 = segment(a)
                s2 = segment(b)
                msg += "\nFrom #1: '{}'\nFrom #2: '{}'".format(s1, s2)
                self.fail(msg)
