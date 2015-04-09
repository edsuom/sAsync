# sAsync:
# An enhancement to the SQLAlchemy package that provides persistent
# dictionaries, text indexing and searching, and an access broker for
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
Mock objects
"""

import random
import zope.interface
from twisted.internet import reactor, defer
from asynqueue import IWorker


VERBOSE = False


class MockTask(object):
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


class MockWorker(object):
    zope.interface.implements(IWorker)

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
        if VERBOSE:
            ID = getattr(self, 'ID', 0)
            print "Worker %d ran %s = %s" % (ID, str(self.task), result)
        self.task.d.callback(result)

    def stop(self):
        self.isShutdown = True
        if VERBOSE:
            print "Shutting down worker %s" % self
        d = getattr(getattr(self, 'task', None), 'd', None)
        if d is None or d.called:
            d_shutdown = defer.succeed(None)
        else:
            d_shutdown = defer.Deferred()
            d.chainDeferred(d_shutdown)
        return d_shutdown

    def crash(self):
        pass


class MockThing:
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

