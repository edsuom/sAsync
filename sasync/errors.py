"""
Errors relating to database access


About sAsync
============
B{sAsync} is an enhancement to the SQLAlchemy package that provides
persistent item-value stores, arrays, and dictionaries, and an access
broker for conveniently managing database access, table setup, and
transactions. Everything can be run in an asynchronous fashion using
the Twisted framework and its deferred processing capabilities.

Copyright (C) 2006-2007, 2015 by Edwin A. Suominen,
U{http://edsuom.com/sAsync}


Licensing
=========

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see U{http://www.gnu.org/licenses/}.


"""

class AsyncError(Exception):
    """
    The requested action is incompatible with asynchronous operations.
    """

class TransactionError(Exception):
    """
    An exception was raised while trying to run a transaction.
    """

    
class DatabaseError(Exception):
    """
    A problem occured when trying to access the database.
    """

class TransactIterationError(Exception):
    """
    An attempt to access a transaction result as an iterator
    """
