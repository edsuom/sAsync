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
Convenient select-object usage for asynchronous database
transactions via C{SQLAlchemy}.
"""

from twisted.internet import defer

import sqlalchemy as SA


class SelectAndResultHolder(object):
    """
    I am yielded by L{database.AccessBroker.selectorator} to let you
    work on (1) a C{select} of the provided columns or (2) an object
    produced by a callable and any args for it, then call me for its
    result.

    Provide my constructor with a reference to the
    L{database.AccessBroker} and the args, plus any keywords you want
    added to the L{call}.
    
    Everything is cleaned up via my L{close} method after the "loop"
    ends.
    """
    def __init__(self, broker, *args, **kw):
        self.broker = broker
        if callable(args[0]):
            self._sObject = args[0](*args[1:])
        else:
            self._sObject = SA.select(args)
        self.kw = kw

    def _wrapper(self, *args, **kw):
        """
        Replaces the C{select} object with the result of a method of it that
        you obtained as an attribute of me. Henceforth my attributes
        shall be those of the replacement object.
        """
        self._sObject = getattr(self._sObject, self._methodName)(*args, **kw)
        
    def __getattr__(self, name):
        """
        Access an attribute of my C{select} object (or a replacement obtained
        via a method call) as if it were my own. If the attribute is
        callable, wrap it in my magic object-replacement wrapper
        method.
        """
        obj = getattr(self._sObject, name)
        if callable(obj):
            self._methodName = name
            return self._wrapper
        return obj

    def __call__(self, *args, **kw):
        """
        Executes the C{select} object, with any supplied args and keywords.

        If you call this from within a transaction already, the
        nesting will be dealt with appropriately and you will get an
        immediate C{ResultProxy}. Otherwise, you'll get a deferred that
        fires with the result, with row iteration coolness.

        As with any transaction, you can disable such behavior and get
        either the raw C{ResultProxy} (with I{raw}) or a list of rows
        (with I{asList}). Those transaction keywords can get supplied
        to my constructor or to this call, if it doesn't itself occur
        from inside a transaction.
        """
        kw.update(self.kw)
        self.result = self.broker.execute(self._sObject, *args, **kw)
        return self.result

    def close(self):
        """
        Closes the C{ResultProxy} if possible.
        """
        def closer(rp):
            rp.close()
            return rp
        
        result = getattr(self, 'result', None)
        if isinstance(result, defer.Deferred):
            result.addCallback(closer)
        elif callable(getattr(result, 'close', None)):
            result.close()
