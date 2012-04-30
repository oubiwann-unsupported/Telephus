Cassanova
=========

Cassanova is a fake Cassandra cluster. It runs in a single Python thread and
is very lightweight for startup and shutdown- making it particularly useful
for automated testing of Cassandra client interfaces and libraries.

Features:

- Passes most of the Cassandra Thrift system tests!
- Can mimic multiple nodes on different network interfaces
- Easy to start and stop
- Implemented as a Twisted Service, so it can share a process and reactor
  with your own Twisted code

What it does *not* support (yet?):

- Partitioners other than RandomPartitioner
- Indexes
- Column validators or custom comparators
- TTLs
- Consistency levels (there's only one actual data store between all "nodes")
- Write to disk (everything is lost when the process exits)
- CQL (durr)
- describe_splits (durr)
- Perform efficiently (data is stored according to what was easiest to write,
  not what would be most efficient for writes or lookups)


Using
-----

Requirements: Python (2.5 or higher), Twisted Matrix (8.0 or higher, I think),
Thrift python library

The simplest way to run Cassanova is with the ``Makefile`` at the top-level of
the Telephes codebase::

    make run-fake-cassandra

This runs in the background, calling a convenience wrapper, ``bin/cassandra``,
which sets a PID file and logfile. It, in turn, calls a Twisted ``.tac`` file
which imports the code from this subpackage. If you want to run the ``.tac``
file directly, you can do the standard Twisted call for that::

    twistd -noy ./bin/cassandra.tac

This will run in the foreground.

To change the port that this server runs on, set the
``$CASSANOVA_CLUSTER_PORT`` environment variable.

To run the test suite, have a Cassandra 0.7 source tree handy (I've been
working against the 0.7.3 branch, so I know it works there), make sure the
``nose`` test coverage tool is installed, and run::

    make check-cassanova


Code Overview
-------------

All the real code is in ``telephus.testing.cassanova``. The more important
classes are:

- ``api.Cassanova``: Provides the implementation of each of the
  supported Thrift calls. Corresponds to a single Thrift connection to the
  service.
- ``server.CassanovaNode``: Corresponds to a single (fake) Cassandra node, listening
  on its own interface. You'll need to have a local network interface for each
  node you want to run.
- ``service.CassanovaService``: The central service, which holds the schema and data.
  Corresponds to a whole cluster. The ``add_node`` method is used to
  instantiate new listening ``api.CassanovaNode``\s, given a unique network
  interface.


Data Storage
------------

``service.CassanovaService`` stores info about keyspaces and column family
definitions on the keyspaces attribute, separate from data.

Actual data is stored in the data attribute, which is a dictionary mapping
keyspace names to what I'll call keyspace-dicts.

keyspace-dicts map ColumnFamily names to cf-dicts.

cf-dicts map keys to row-dicts. In addition, the special None key in each
cf-dict is mapped to its own ColumnFamily name.

row-dicts map column names to Column objects, if in a standard
``ColumnFamily``, or supercolumn names to supercolumn-dicts otherwise. In
addition, the special None key in each row-dict is mapped to its own row key.

supercolumn-dicts map column names to Column objects. In addition, the
special None key in each supercolumn-dict is mapped to its own supercolumn
name.
