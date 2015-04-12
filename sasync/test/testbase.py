# sAsync:
# An enhancement to the SQLAlchemy package that provides persistent
# item-value stores, arrays, and dictionaries, and an access broker for
# conveniently managing database access, table setup, and
# transactions. Everything can be run in an asynchronous fashion using
# the Twisted framework and its deferred processing capabilities.
#
# Copyright (C) 2006-7, 2015 by Edwin A. Suominen, http://edsuom.com
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
Mock objects and an improved TestCase for sAsync
"""

import re, sys, os.path, logging

from zope.interface import implements
from twisted.internet import reactor, defer
from twisted.python.failure import Failure
from twisted.internet.interfaces import IConsumer
from twisted.trial import unittest

from asynqueue.interfaces import IWorker


VERBOSE = False


def deferToDelay(delay):
    d = defer.Deferred()
    reactor.callLater(delay, d.callback, None)
    return d


class MsgBase(object):
    """
    A mixin for providing a convenient message method.
    """
    def isVerbose(self):
        if hasattr(self, 'verbose'):
            return self.verbose
        if 'VERBOSE' in globals():
            return VERBOSE
        return False

    def msg(self, proto, *args):
        if self.isVerbose():
            if not hasattr(self, 'msgAlready'):
                proto = "\n" + proto
                self.msgAlready = True
            if args and args[-1] == "-":
                args = args[:-1]
                proto += "\n{}".format("-"*40)
            print proto.format(*args)


class TestHandler(MsgBase, logging.StreamHandler):
    def __init__(self, verbose=False):
        logging.StreamHandler.__init__(self)
        self.verbose = verbose
        self.records = []
        self.setFormatter(logging.Formatter(
            '%(levelname)s: %(message)s'))
        
    def emit(self, record):
        self.records.append(record)
        if self.verbose:
            return logging.StreamHandler.emit(self, record)

            
class MockTask(MsgBase):
    def __init__(self, f, args, kw, priority, series):
        self.ran = False
        self.callTuple = (f, args, kw)
        self.priority = priority
        self.series = series
        self.d = defer.Deferred()
    
    def __cmp__(self, other):
        if other is None:
            return -1
        return cmp(self.priority, other.priority)

    def __str__(self):
        return str(self.callTuple[0])


class MockWorker(MsgBase):
    implements(IWorker)

    def __init__(self, runDelay=0.0):
        self.runDelay = runDelay
        self.ran = []
        self.isShutdown = False

    def run(self, task):
        def ran(result, d):
            d.callback(None)
            return result
        
        self.task = task
        reactor.callLater(self.runDelay, self._reallyRun)
        d = defer.Deferred()
        task.d.addCallback(ran, d)
        return d
    
    def _reallyRun(self):
        f, args, kw = self.task.callTuple
        result = f(*args, **kw)
        self.ran.append(self.task)
        with self.verboseContext():
            ID = getattr(self, 'ID', 0)
            self.msg("Worker {} ran {} = {}", ID, str(self.task), result)
        self.task.d.callback(result)

    def stop(self):
        self.isShutdown = True
        self.msg("Shutting down worker {}", self)
        d = getattr(getattr(self, 'task', None), 'd', None)
        if d is None or d.called:
            d_shutdown = defer.succeed(None)
        else:
            d_shutdown = defer.Deferred()
            d.chainDeferred(d_shutdown)
        return d_shutdown

    def crash(self):
        pass


class MockThing(MsgBase):
    def __init__(self):
        self.beenThereDoneThat = False
    
    def method(self, x):
        self.beenThereDoneThat = True
        return 2*x

    def __cmp__(self, other):
        if not hasattr(other, 'beenThereDoneThat'):
            # We are superior; we have the attribute and 'other' doesn't!
            return 1
        elif self.beenThereDoneThat and not other.beenThereDoneThat:
            return 1
        elif not self.beenThereDoneThat and other.beenThereDoneThat:
            return -1
        else:
            return 0
            

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

        
class MockConnection(MsgBase):
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.msg("Connection open")
    def begin(self):
        return MockTransaction()

        
class MockQueue(MsgBase):
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

        
class MockBroker(MsgBase):
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
        self.msg(
            "Data received from {}: '{}'", repr(self.producer), str(data))
        if self.writeTime:
            self.producer.pauseProducing()
            self.d = deferToDelay(
                self.writeTime).addCallback(resume)


class TestCase(MsgBase, unittest.TestCase):
    """
    Slightly improved TestCase
    """
    # Nothing should take longer than 10 seconds, and often problems
    # aren't apparent until the timeout stops the test.
    timeout = 10
    
    def oops(self, failureObj, *metaArgs):
        with self.verboseContext():
            if not metaArgs:
                metaArgs = (repr(self),)
            text = info.Info.setCall(*metaArgs).aboutFailure(failureObj)
            self.msg(text)
        return failureObj
    
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

    def assertIsFailure(self, x):
        self.assertIsInstance(x, Failure)
