##sAsync
SQLAlchemy made asynchronous

API Docs: <http://edsuom.com/sAsync/sasync/html>
PyPI Page: <https://pypi.python.org/pypi/sAsync/>
*See also* [AsynQueue](http://edsuom.com/AsynQueue.html)

**sAsync** lets you use the use the database transaction core of the
[SQLAlchemy](http://www.sqlalchemy.org/) Python SQL toolkit in an
asynchronous fashion. In addition to an access broker for conveniently
managing database access, table setup, and transactions under the
[Twisted](http://twistedmatrix.com) framework, it provides persistent
item-value stores, arrays, and dictionaries.

The **sAsync** package uses a threaded task queue from the
[AsynQueue](AsynQueue.html) package to wrap your SQLAlchemy database
access code inside asynchronous transactions. At the lowest level, it
provides a [@transact](sAsync/sasync.database.html#transaction)
decorator for your database-access methods that makes them immediately
return a Twisted C{Deferred} object.

For example, suppose you want to run a method that selects a list of
row objects from a table. Instead of waiting around for your method to
return the list, blocking everything else your program is trying to
do, you decorate it with C{@transact} and run it. It immediately hands
you a C{Deferred} object that promises an eventual result. You add one
or more callback functions to the C{Deferred} and C{Twisted} will call
them when the database transaction is finally run from the queue.

Once you've attached your callback function to the deferred result,
you can go on with your business, knowing that SQLAlchemy will be
cranking away behind the scenes (in a transaction-specific thread) to
obtain a result for you. When the result is finally ready, your
transact-decorated method will look at the C{Deferred}, see the note
you scribbled on it (*Pls call this function with the result. Thx!*),
and give your function a call with the list of rows. It will supply
the callback with the list as the function's argument.

Actually, it's better than that. Unless you specify otherwise, the 
C{Deferred} object will iterate the rows of your database query in an
asynchronous, Twisted fashion. The result will be an instance of
AsynQueue's [Deferator](AsynQueue/asynqueue.iteration.Deferator.html),
which spits out baby C{Deferred} objects as it iterates over your
result. If you use the *consumer* keyword to supply an object
implementing Twisted's C{IConsumer} interface, the result rows will be
"produced" to it instead.

You can also do some asynchronous database operations on a higher
level. For example, you can maintain a store of Python objects, with
each object accessible (with deferred results) via a unique key. If
that sounds like what a dictionary does, it should! The sAsync package
also provides a dictionary-like object with database-persistent items
that you can access in an asynchronous fashion. Check out the
[PersistentDict](sAsync/sasync.pdict.PersistentDict.html) object,
which builds on the [Items](sAsync/sasync.items.Items.html) persistent
store of name-value items. The [items.py](sAsync/items.py.html) source
file provides a nice simple example of how to use sAsync.


###Table Setup

Relational databases operate on tables, and setting up the tables in
your database can be a huge pain in the ass. But the job is easier
with sAsync's
[AccessBroker.table](sAsync/sasync.database.AccessBroker.html#table)
method. It creates the table you specify if none exists yet and sets
up indices for it, as you direct it. Once the table is in your
database, you can reference it as an attribute of your C{AccessBroker}
object.

The sensible place to do table setup is at the very beginning, before
any database transactions get underway. You can define a C{startup}
method of your C{AccessBroker}, and it will get run when the object is
constructed.

Here's the C{startup} method for the database access broker in my
logfile-to-database parsing application *statalysis*:

    :::python
    @defer.inlineCallbacks
    def startup(self):
        # Primary key is an auto-incrementing index, which can be used
        # to find out the order in which requests were made within a
        # single second.
        yield self.table(
            'entries',
            SA.Column('id', SA.Integer, primary_key=True),
            SA.Column('dt', SA.DateTime),
            SA.Column('ip', SA.String(15)),
            SA.Column('http', SA.SmallInteger),
            SA.Column('was_rd', SA.Boolean),
            SA.Column('id_vhost', SA.Integer),
            SA.Column('id_url', SA.Integer),
            SA.Column('id_ref', SA.Integer),
            SA.Column('id_ua', SA.Integer),
            index_dt=['dt'], index_ip=['ip']
        )
        for name in self.indexedValues:
            yield self.table(
                name,
                SA.Column('id', SA.Integer, primary_key=True),
                SA.Column('value', SA.String(self.valueLength)),
                unique_value=['value']
            )
        yield self.table(
            'bad_ip',
            SA.Column('ip', SA.String(15), primary_key=True),
            SA.Column('purged', SA.Boolean, nullable=False),
        )
        yield self.table(
            'files',
            SA.Column(
                'name', SA.String(self.valueLength), primary_key=True),
            SA.Column('dt', SA.DateTime),
            SA.Column('records', SA.Integer),
        )
        self.pendingID = {}
        self.dtk = DTK()
        self.ipm = IPMatcher()
        self.ipList = []
        self.idTable = {}
        for name in self.indexedValues:
            self.idTable[name] = {}


###Deferred Results

Notice all the C{yield} statements? Twisted's C{inlineCallbacks}
capability makes it easy to use C{Deferred} objects. You don't need to
add all those callbacks and a pile of callback functions one after the
other. Just decorate your method with C{@defer.inlineCallbacks} (after
doing "C{from twisted.internet import defer}," of course) and yield the 
C{Deferred} objects. Processing will resume in the method when the 
C{Deferred} fires.

In the C{startup} method, there is nothing obtaining any deferred
results. The C{Deferred} objects from each call to *self.table* are
just yielded for asynchronous program flow and that's that. Each one
fires when a table has been made (or checked), with no result except
the fact that setup work is done for that particular table.

With inline callbacks, you can get the value that a C{Deferred} has
been fired with. Often there's important information there. Here's an
example of how that is done, in a method of the *statalysis* access
broker that actually accesses database content:

    :::python
    @defer.inlineCallbacks
    def setRecord(self, dt, record):
        """
        Adds all needed database entries for the supplied record at the
        specified datetime.

        @return: A C{Deferred} that fires with a bool indicating if a
          new entry was added.
        
        """
        ip = record['ip']
        if ip in self.ipList:
            # Ignore this, it's from an already purged IP address
            result = False
        else:
            self.ipm.addIP(ip)
            # Build list of values and indexed-value IDs
            values = [record[x] for x in self.directValues]
            for name in self.indexedValues:
                value = record[name][:self.valueLength]
                if value in self.idTable[name]:
                    # We've set this value already
                    ID = self.idTable[name][value]
                else:
                    ID = yield self.setNameValue(name, value, niceness=-15)
                    # Add to idTable for future reference, avoiding DB checks
                    self.idTable[name][value] = ID
                values.append(ID)
            # With this next line commented out and result = False
            # instead, the memory leak still persists. CPU time for the
            # main process was 66% of normal.
            result = yield self.setEntry(dt, values)
        defer.returnValue(result)

The ID value for an indexed value, when not found in the in-memory
*idTable*, is obtained from the transaction method C{setNameValue}. It
returns a deferred result, and that is put into the local C{ID}
variable via the inline callback.


###Transaction Methods

So, what does C{setNameValue} look like? I thought you'd never ask.

    :::python
    @transact
    def setNameValue(self, name, value):
        """
        Get the unique ID for this value in the named table, adding a new
        entry for it there if necessary.
        """
        table = getattr(self, name)
        if not self.s("s_{}".format(name)):
            self.s([table.c.id], table.c.value == SA.bindparam('value'))
        ID = self.s().execute(value=value).scalar()
        if ID is None:
            rp = table.insert().execute(value=value)
            ID = rp.lastrowid
            rp.close()
        return ID

Now we get to see a little sAsync magic. Look at that
[@transact](sAsync/sasync.database.html#transact) decorator. It makes
the method into a transaction, running it via the thread queue and
with its own SQLAlchemy begin/commit setup for the database
connection. There's a lot going on with that little decorator; take a
look at the source for [database.py](sAsync/database.py.html) to get
some idea of what's under the hood.

Another bit of coolness in C{setNameValue} is the polymorphic method
C{s} (of
[sasync.database.Accessbroker](sAsync/sasync.database.AccessBroker.html#s)). It
lets you compile and save an SQLAlchemy C{select} object and then run
it with new parameters whenever you want.


###License

Copyright (C) 2006-2007, 2015 by Edwin A. Suominen,
http://edsuom.com/sAsync

See **edsuom.com** for API documentation as well as information about
Ed's background and other projects, software and otherwise.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the
License. You may obtain a copy of the License at

  <http://www.apache.org/licenses/LICENSE-2.0>

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an "AS
IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language
governing permissions and limitations under the License.
