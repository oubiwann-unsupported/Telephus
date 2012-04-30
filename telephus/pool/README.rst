Cluster Pooling
===============

This subpackage provides functionality that lets you spread requests among
connections to multiple nodes in a Cassandra cluster.


Quick Start
-----------

Here's some example usage::

    >>> from telephus.pool.service import CassandraClusterPool
    >>>
    >>> my_seed_nodes = ['192.168.2.14', '192.168.2.15', '192.168.2.16']
    >>> mypool = CassandraClusterPool(
    ...   my_seed_nodes, keyspace='MyKeyspace', pool_size=10)
    >>> mypool.startService()
    >>> mypool.get('Key12345', 'SomeCF')
    <Deferred at 0x1b2b248>

``CassandraClusterPool`` will respond to all the methods on CassandraClient,
but if you prefer to have separate ``CassandraClient`` instances, set your pool
object as their manager.


Some Tips
---------

Some of the most useful additional methods on ``CassandraClusterPool``:

* ``adjustPoolSize(newsize)`` - change the size of the connection pool,
  without interrupting ongoing requests
* ``addNode((address, port))`` - manually add another node to the pool.
  Normally this shouldn't be necessary; once the pool can connect to one or
  more of your your seed nodes, it can inspect the ring and find the rest of
  the nodes.
* ``removeNode((address, port))`` - manually remove a node from the pool. It
  will be re-added later if it shows up in the ring with a subsequent
  connection, though.
* ``set_keyspace(ksname)`` - change the keyspace used for future requests on
  this pool


TODO
----

* check cluster name on connecting to each new host, to make sure it's
  actually in the same cluster
* take node error/connection history into account with add_connection_score
* remove nodes that have been missing or unconnectable for too long
* when seed node list is shofter than requested pool size, don't try to
  fill the pool completely until after a seed node is contacted and an
  initial live-node list collected
