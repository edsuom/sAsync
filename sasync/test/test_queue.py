# sAsync:
# An enhancement to the SQLAlchemy package that provides persistent
# dictionaries, text indexing and searching, and an access broker for
# conveniently managing database access, table setup, and
# transactions. Everything can be run in an asynchronous fashion using the
# Twisted framework and its deferred processing capabilities.
#
# Copyright (C) 2006 by Edwin A. Suominen, http://www.eepatents.com
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

    @defer.inlineCallbacks
    def test_setAndGet(self):
        url = "sqlite://"
        qf = queue.Factory()
        q1 = yield qf(url)
        q2 = yield qf(url)
        self.assertEqual(q1, q2)
        
        