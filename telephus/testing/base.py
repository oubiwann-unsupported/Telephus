import contextlib

from twisted.internet import defer
from twisted.trial import unittest

from telephus.cassandra import ttypes
from telephus.testing import fake


class FunctionalTestCaseBase(unittest.TestCase):
    """
    """

class UseCassandraTestCaseBase(FunctionalTestCaseBase):
    """
    """
    start_port = 44449
    ksname = 'TestKeyspace'

    def setUp(self):
        super(UseCassandraTestCaseBase, self).setUp()
        # clean up any keyspaces that were left around from tests that didn't
        # previously shutdown properly

    def assertFired(self, d):
        self.assert_(d.called, msg='%s has not been fired' % (d,))

    def assertNotFired(self, d):
        self.assertNot(d.called, msg='Expected %s not to have been fired, but'
                                     ' it has been fired.' % (d,))

    def assertNumConnections(self, num):
        conns = self.cluster.get_connections()
        self.assertEqual(len(conns), num,
                         msg='Expected %d existing connections to cluster, but'
                             ' %d found.' % (num, len(conns)))
        return conns

    def assertNumUniqueConnections(self, num):
        conns = self.cluster.get_connections()
        conns = set(n for (n,p) in conns)
        self.assertEqual(len(conns), num,
                         msg='Expected %d unique nodes in cluster with existing'
                             ' connections, but %d found. Whole set: %r'
                             % (num, len(conns), sorted(conns)))
        return conns

    def assertNumWorkers(self, num):
        workers = self.cluster.get_working_connections()
        self.assertEqual(len(workers), num,
                         msg='Expected %d pending requests being worked on in '
                             'cluster, but %d found' % (num, len(workers)))
        return workers

    def killSomeConn(self):
        conns = self.cluster.get_connections()
        self.assertNotEqual(len(conns), 0)
        node, proto = conns[0]
        proto.transport.loseConnection()
        return proto

    def killSomeNode(self):
        conns = self.cluster.get_connections()
        self.assertNotEqual(len(conns), 0)
        node, proto = conns[0]
        node.stopService()
        return node

    def killWorkingConn(self):
        conns = self.cluster.get_working_connections()
        self.assertNotEqual(len(conns), 0)
        node, proto = conns[0]
        proto.transport.loseConnection()
        return proto

    def killWorkingNode(self):
        conns = self.cluster.get_working_connections()
        self.assertNotEqual(len(conns), 0)
        node, proto = conns[0]
        node.stopService()
        return node

    @contextlib.contextmanager
    def cluster_and_pool(self, num_nodes=10, pool_size=5, start=True,
                         cluster_class=None, api_version=None):
        from telephus.pool import CassandraClusterPool
        if cluster_class is None:
            cluster_class = fake.FakeCassandraCluster
        cluster = cluster_class(num_nodes, start_port=self.start_port)
        pool = CassandraClusterPool(
            [cluster.iface], thrift_port=self.start_port, pool_size=pool_size,
            api_version=api_version)
        if start:
            cluster.startService()
            pool.startService()
        self.cluster = cluster
        self.pool = pool
        try:
            yield cluster, pool
        finally:
            del self.pool
            del self.cluster
            if pool.running:
                pool.stopService()
            if cluster.running:
                cluster.stopService()

    @defer.inlineCallbacks
    def make_standard_cfs(self, ksname=None):
        if ksname is None:
            ksname = self.ksname
        yield self.pool.system_add_keyspace(
            ttypes.KsDef(
                name=ksname,
                replication_factor=1,
                strategy_class='org.apache.cassandra.locator.SimpleStrategy',
                cf_defs=(
                    ttypes.CfDef(
                        keyspace=ksname,
                        name='Standard1',
                        column_type='Standard'
                    ),
                    ttypes.CfDef(
                        keyspace=ksname,
                        name='Super1',
                        column_type='Super'
                    )
                )
            )
        )
        yield self.pool.set_keyspace(ksname)
        yield self.pool.insert('key', 'Standard1', column='col', value='value')

    @defer.inlineCallbacks
    def insert_dumb_rows(self, ksname=None, cf=None, numkeys=10, numcols=10,
                         timestamp=0):
        if ksname is None:
            ksname = self.ksname
        if cf is None:
            cf = 'Standard1'
        yield self.pool.set_keyspace(ksname)

        mutmap = {}
        for k in range(numkeys):
            key = 'key%03d' % k
            cols = [ttypes.Column(name='%s-%03d-%03d' % (ksname, k, n),
                                  value='val-%s-%03d-%03d' % (ksname, k, n),
                                  timestamp=timestamp)
                    for n in range(numcols)]
            mutmap[key] = {cf: cols}
        yield self.pool.batch_mutate(mutationmap=mutmap)


class UnitTestCaseBase(unittest.TestCase):
    """
    """
