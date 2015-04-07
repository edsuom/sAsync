# sAsync:
# An enhancement to the SQLAlchemy package that provides persistent
# dictionaries, text indexing and searching, and an access broker for
# conveniently managing database access, table setup, and
# transactions. Everything is run in an asynchronous fashion using the Twisted
# framework and its deferred processing capabilities.
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
Asynchronous database transactions via SQLAlchemy.
"""

import sqlalchemy as SA


class SelectAndResultHolder(object):
    """
    I am yielded by L{AccessBroker.selectorator} to let you work on
    (1) a select of the provided columns or (2) an object produced by
    a callable and any args for it, then call me for its
    result.

    Everything is cleaned up via my L{close} method after the "loop"
    ends.
    """
    def __init__(self, conn, *args):
        self.conn = conn
        if callable(args[0]):
            self._sObject = args[0](*args[1:])
        else:
            self._sObject = SA.select(args)

    def _wrapper(self, *args, **kw):
        """
        Replaces the select object with the result of a method of it that
        you obtained as an attribute of me. Henceforth my attributes
        shall be those of the replacement object.
        """
        self._sObject = getattr(self._sObject, self._methodName)(*args, **kw)
        
    def __getattr__(self, name):
        """
        Access an attribute of my select object (or a replacement obtained
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
        Call my connection to execute the select object with any supplied
        args and keywords. The ResultProxy is returned and also
        replaces the select object. Thus, after this call, you can
        access its attributes as if they were my own, if that turns you on.
        """
        self._sObject = self.conn.execute(self._sObject, *args, **kw)
        return self._sObject

    def close(self):
        self._sObject.close()