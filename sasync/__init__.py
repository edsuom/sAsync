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
An enhancement to the C{SQLAlchemy} package that provides
persistent item-value stores, arrays, and dictionaries, and an access
broker for conveniently managing database access, table setup, and
transactions. Everything can be run in an asynchronous fashion using
the Twisted framework and its deferred processing capabilities.

Start with L{database.AccessBroker}.
"""

def engine(url, **kw):
    """
    Specifies the parameters for creating an SQLAlchemy database
    engine that will be used as a default for all instances of
    L{AccessBroker} and all persistent objects based thereon.

    @see: U{http://docs.sqlalchemy.org/en/rel_1_0/core/engines.html}.

    @param url: An RFC-1738 url to a database connection.
          
    @keyword strategy: The Strategy describes the general
      configuration used to create this Engine. The two available
      values are plain, which is the default, and threadlocal, which
      applies a 'thread-local context' to implicit executions
      performed by the Engine. This context is further described in
      Implicit Connection Contexts.

    @type strategy: 'plain'.

    @keyword pool: An instance of sqlalchemy.pool.Pool to be used as
      the underlying source for connections, overriding the engine's
      connect arguments (pooling is described in Connection
      Pooling). If C{None}, a default Pool (usually QueuePool, or
      SingletonThreadPool in the case of SQLite) will be created using
      the engine's connect arguments.

    @type pool: C{None}

    @keyword pool_size: The number of connections to keep open inside
      the connection pool. This is only used with QueuePool.

    @type pool_size: 5

    @keyword max_overflow: The number of connections to allow in
      'overflow,' that is connections that can be opened above and
      beyond the initial five. This is only used with QueuePool.

    @type max_overflow: 10
    
    @keyword pool_timeout: number of seconds to wait before giving up
      on getting a connection from the pool. This is only used with
      QueuePool.

    @type pool_timeout: 30

    @keyword echo: if C{True}, the Engine will log all statements as
      well as a repr() of their parameter lists to the engines logger,
      which defaults to sys.stdout. The echo attribute of
      ComposedSQLEngine can be modified at any time to turn logging on
      and off. If set to the string 'debug', result rows will be
      printed to the standard output as well.

    @type echo: C{False}

    @keyword module: used by database implementations which support
      multiple DBAPI modules, this is a reference to a DBAPI2 module
      to be used instead of the engine's default module. For Postgres,
      the default is psycopg2, or psycopg1 if 2 cannot be found. For
      Oracle, its cx_Oracle.

    @type module: C{None}

    @keyword use_ansi: used only by Oracle; when C{False}, the Oracle
      driver attempts to support a particular 'quirk' of Oracle
      versions 8 and previous, that the LEFT OUTER JOIN SQL syntax is
      not supported, and the 'Oracle join' syntax of using
      <column1>(+)=<column2> must be used in order to achieve a LEFT
      OUTER JOIN.

    @type use_ansi: C{True}

    @keyword threaded: used by cx_Oracle; sets the threaded parameter
      of the connection indicating thread-safe usage. cx_Oracle docs
      indicate setting this flag to C{False} will speed performance by
      10-15%. While this defaults to C{False} in cx_Oracle, SQLAlchemy
      defaults it to C{True}, preferring stability over early
      optimization.

    @type threaded: C{True}

    @keyword use_oids: used only by Postgres, will enable the column
      name 'oid' as the object ID column, which is also used for the
      default sort order of tables. Postgres as of 8.1 has object IDs
      disabled by default.

    @type use_oids: C{False}

    @keyword convert_unicode: if set to C{True}, all String/character
      based types will convert Unicode values to raw byte values going
      into the database, and all raw byte values to Python Unicode
      coming out in result sets. This is an engine-wide method to
      provide unicode across the board. For unicode conversion on a
      column-by-column level, use the Unicode column type instead.

    @type convert_unicode: C{False}

    @keyword encoding: the encoding to use for all Unicode
      translations, both by engine-wide unicode conversion as well as
      the Unicode type object.

    @type encoding: 'utf-8'
    """
    from queue import Factory
    Factory.setGlobal(url, **kw)
    
