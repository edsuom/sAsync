# sAsync:
# An enhancement to the SQLAlchemy package that provides persistent
# item-value stores, arrays, and dictionaries, and an access broker for
# conveniently managing database access, table setup, and
# transactions. Everything can be run in an asynchronous fashion using
# the Twisted framework and its deferred processing capabilities.
#
# Copyright (C) 2015 by Edwin A. Suominen, http://edsuom.com
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
Unit tests for sasync.queue

Every damn little thing needs its own tests. When will I ever learn
that?
"""

from twisted.internet import defer

import sqlalchemy as SA

import queue
from testbase import TestCase


class TestFactory(TestCase):
    verbose = True

    @defer.inlineCallbacks
    def test_setAndGetGlobal(self):
        url = "sqlite://"
        qf = queue.Factory()
        qGlobal = yield qf.setGlobal(url)
        self.assertIsInstance(qGlobal.engine, SA.engine.Engine)
        q = yield qf.getGlobal()
        self.assertEqual(q, qGlobal)
        qDefault = yield qf()
        self.assertEqual(qDefault, qGlobal)
        yield q.shutdown()

    @defer.inlineCallbacks
    def test_setAndGet(self):
        url = "sqlite://"
        qf = queue.Factory()
        q1 = yield qf(url)
        q2 = yield qf(url)
        self.assertEqual(q1, q2)
        yield q1.shutdown()
        
