from __future__ import with_statement

import contextlib
from itertools import groupby
import random
from time import time

from twisted.trial import unittest
from twisted.internet import defer, reactor
from twisted.python import log

from thrift.transport import TTransport

from telephus import translate
from telephus.cassandra.c08 import Cassandra
from telephus.cassandra.ttypes import *
from telephus.pool.service import CassandraClusterPool
from telephus.pool.protocol import (
    CassandraPoolParticipantClient, CassandraPoolReconnectorFactory)

from telephus.testing import base, fake
from telephus.testing.cassanova import server as cassanova_server
from telephus.testing.util import addtimeout, deferwait


class CassandraClusterPoolTest(base.UseCassandraTestCaseBase):

    @defer.inlineCallbacks
    def test_set_keyspace(self):
        pool_size=10
        num_nodes=4

        with self.cluster_and_pool(num_nodes=num_nodes, pool_size=pool_size):
            yield self.make_standard_cfs('KS1')
            yield self.make_standard_cfs('KS2')

            yield self.insert_dumb_rows('KS1', numcols=pool_size+2)
            yield self.insert_dumb_rows('KS2', numcols=pool_size+2)

            yield self.pool.set_keyspace('KS1')
            first = self.pool.get('key000', 'Standard1/wait=2.0', 'KS1-000-000')

            yield self.pool.set_keyspace('KS2')
            dfrds1 = []
            for x in range(pool_size + 1):
                d = self.pool.get('key001', 'Standard1/wait=0.1', 'KS2-001-%03d' % x)
                dfrds1.append(d)

            # all pool connections should have sent a real set_keyspace by
            # now; change it up again

            yield self.pool.set_keyspace('KS1')
            dfrds2 = []
            for x in range(pool_size + 1):
                d = self.pool.get('key002', 'Standard1/wait=0.1', 'KS1-002-%03d' % x)
                dfrds2.append(d)

            result = yield defer.DeferredList(dfrds1, consumeErrors=True)
            for n, (succ, res) in enumerate(result):
                self.assert_(succ, 'Failure on item %d was %s' % (n, res))
                res = res.column.value
                self.assertEqual(res, 'val-KS2-001-%03d' % n)

            result = yield defer.DeferredList(dfrds2)
            for n, (succ, res) in enumerate(result):
                self.assert_(succ, 'Failure was %s' % res)
                res = res.column.value
                self.assertEqual(res, 'val-KS1-002-%03d' % n)

            yield self.pool.set_keyspace('KS2')

            result = (yield first).column.value
            self.assertEqual(result, 'val-KS1-000-000')

            final = yield self.pool.get('key003', 'Standard1', 'KS2-003-005')
            self.assertEqual(final.column.value, 'val-KS2-003-005')

    @defer.inlineCallbacks
    def test_bad_set_keyspace(self):
        with self.cluster_and_pool():
            yield self.make_standard_cfs('KS1')
            yield self.insert_dumb_rows('KS1')

            yield self.assertFailure(self.pool.set_keyspace('i-dont-exist'),
                                     InvalidRequestException)
            self.flushLoggedErrors()

            # should still be in KS1
            result = yield self.pool.get('key005', 'Standard1', 'KS1-005-000')
            self.assertEqual(result.column.value, 'val-KS1-005-000')

    @defer.inlineCallbacks
    def test_ring_inspection(self):
        with self.cluster_and_pool(start=False):
            self.assertEqual(len(self.pool.seed_list), 1)
            self.cluster.startService()
            self.pool.startService()
            yield self.pool.describe_cluster_name()
            self.assertEqual(len(self.pool.nodes), len(self.cluster.ring))

    @defer.inlineCallbacks
    def test_keyspace_connection(self):
        numkeys = 10
        numcols = 10
        tries = 500

        with self.cluster_and_pool():
            yield self.make_standard_cfs('KS1')
            yield self.make_standard_cfs('KS2')
            yield self.insert_dumb_rows('KS1', numkeys=numkeys, numcols=numcols)
            yield self.insert_dumb_rows('KS2', numkeys=numkeys, numcols=numcols)

            ksconns = dict((ksname, self.pool.keyspaceConnection(ksname))
                           for ksname in ('KS1', 'KS2'))

            dlist = []
            for i in xrange(tries):
                keyspace = 'KS%d' % random.randint(1, 2)
                keynum = '%03d' % random.randint(0, numkeys-1)
                key = 'key' + keynum
                col = '%s-%s-%03d' % (keyspace, keynum, random.randint(0, numcols-1))
                d = ksconns[keyspace].get(key, 'Standard1', col)
                d.addCallback(lambda c: c.column.value)
                d.addCallback(self.assertEqual, 'val-' + col)
                dlist.append(d)
            results = yield defer.DeferredList(dlist, consumeErrors=True)
            for succ, answer in results:
                if not succ:
                    answer.raiseException()

    @defer.inlineCallbacks
    def test_storm(self):
        numkeys = 10
        numcols = 10
        tries = 500

        with self.cluster_and_pool():
            yield self.make_standard_cfs()
            yield self.insert_dumb_rows(numkeys=numkeys, numcols=numcols)

            dlist = []
            for i in xrange(tries):
                keynum = '%03d' % random.randint(0, numkeys-1)
                key = 'key' + keynum
                col = '%s-%s-%03d' % (self.ksname, keynum, random.randint(0, numcols-1))
                d = self.pool.get(key, 'Standard1', col)
                d.addCallback(lambda c: c.column.value)
                d.addCallback(self.assertEqual, 'val-' + col)
                dlist.append(d)
            results = yield defer.DeferredList(dlist, consumeErrors=True)
            for succ, answer in results:
                if not succ:
                    answer.raiseException()

    @defer.inlineCallbacks
    def test_retrying(self):
        with self.cluster_and_pool():
            yield self.make_standard_cfs()
            yield self.insert_dumb_rows()

            d = self.pool.get('key000', 'Standard1/wait=1.0', '%s-000-000' % self.ksname,
                              retries=3)

            # give the timed 'get' a chance to start
            yield deferwait(0.05)

            workers = self.assertNumWorkers(1)
            self.killWorkingConn()

            # allow reconnect
            yield deferwait(0.1)

            newworkers = self.assertNumWorkers(1)

            # we want the preference to be reconnecting the same node
            self.assertEqual(workers[0][0], newworkers[0][0])
            answer = (yield d).column.value
            self.assertEqual(answer, 'val-%s-000-000' % self.ksname)
        self.flushLoggedErrors()

    @defer.inlineCallbacks
    def test_resubmit_to_new_conn(self):
        pool_size = 8

        with self.cluster_and_pool(pool_size=1):
            yield self.make_standard_cfs()
            yield self.insert_dumb_rows()

            # turn up pool size once other nodes are known
            self.pool.adjustPoolSize(pool_size)
            yield deferwait(0.1)

            d = self.pool.get('key005', 'Standard1/wait=1.0', '%s-005-000' % self.ksname,
                              retries=3)

            # give the timed 'get' a chance to start
            yield deferwait(0.1)

            workers = self.assertNumWorkers(1)
            node = self.killWorkingNode()

            # allow reconnect
            yield deferwait(0.5)
            newworkers = self.assertNumWorkers(1)

            # reconnect should have been to a different node
            self.assertNotEqual(workers[0][0], newworkers[0][0])

            answer = (yield d).column.value
            self.assertEqual(answer, 'val-%s-005-000' % self.ksname)

        self.flushLoggedErrors()

    @defer.inlineCallbacks
    def test_adjust_pool_size(self):
        pool_size = 8
        diminish_by = 2

        with self.cluster_and_pool(pool_size=1):
            yield self.make_standard_cfs()
            yield self.insert_dumb_rows()

            # turn up pool size once other nodes are known
            self.pool.adjustPoolSize(pool_size)
            yield deferwait(0.1)

            self.assertNumConnections(pool_size)
            self.assertNumUniqueConnections(pool_size)

            dlist = []
            for x in range(pool_size):
                d = self.pool.get('key001', 'Standard1/wait=1.0',
                                  '%s-001-002' % self.ksname, retries=0)
                d.addCallback(lambda c: c.column.value)
                d.addCallback(self.assertEqual, 'val-%s-001-002' % self.ksname)
                dlist.append(d)

            yield deferwait(0.1)

            for d in dlist:
                self.assertNotFired(d)
            self.assertNumConnections(pool_size)
            self.assertNumWorkers(pool_size)
            self.assertNumUniqueConnections(pool_size)

            # turn down pool size
            self.pool.adjustPoolSize(pool_size - diminish_by)
            yield deferwait(0.1)

            # still pool_size conns until the ongoing requests finish
            for d in dlist:
                self.assertNotFired(d)
            self.assertNumConnections(pool_size)
            self.assertEqual(len(self.pool.dying_conns), diminish_by)

            result = yield defer.DeferredList(dlist, consumeErrors=True)
            for succ, answer in result:
                if not succ:
                    answer.raiseException()
            yield deferwait(0.1)

            self.assertNumConnections(pool_size - diminish_by)
            self.assertNumWorkers(0)

    @defer.inlineCallbacks
    def test_zero_retries(self):
        with self.cluster_and_pool():
            yield self.make_standard_cfs()
            yield self.insert_dumb_rows()
            d = self.pool.get('key006', 'Standard1/wait=0.5',
                              '%s-006-002' % self.ksname, retries=0)

            yield deferwait(0.05)
            self.assertNumWorkers(1)

            # kill the connection handling the query- an immediate retry
            # should work, if a retry is attempted
            self.killWorkingConn()

            yield self.assertFailure(d, TTransport.TTransportException)

        self.flushLoggedErrors()

    @defer.inlineCallbacks
    def test_exhaust_retries(self):
        retries = 3
        num_nodes = pool_size = retries + 2

        with self.cluster_and_pool(num_nodes=num_nodes, pool_size=1):
            yield self.make_standard_cfs()
            yield self.insert_dumb_rows()

            # turn up pool size once other nodes are known
            self.pool.adjustPoolSize(pool_size)
            yield deferwait(0.2)

            self.assertNumConnections(pool_size)
            self.assertNumUniqueConnections(pool_size)

            d = self.pool.get('key002', 'Standard1/wait=0.5',
                              '%s-002-003' % self.ksname, retries=retries)
            yield deferwait(0.05)

            for retry in range(retries + 1):
                self.assertNumConnections(pool_size)
                self.assertNumWorkers(1)
                self.assertNotFired(d)
                self.killWorkingNode()
                yield deferwait(0.1)

            yield self.assertFailure(d, TTransport.TTransportException)

        self.flushLoggedErrors()

    @defer.inlineCallbacks
    def test_kill_pending_conns(self):
        num_nodes = pool_size = 8
        fake_pending = 2

        with self.cluster_and_pool(num_nodes=num_nodes, pool_size=1):
            yield self.make_standard_cfs()
            yield self.insert_dumb_rows()

            # turn up pool size once other nodes are known
            self.pool.adjustPoolSize(pool_size)
            yield deferwait(0.1)

            self.assertNumConnections(pool_size)
            self.assertNumUniqueConnections(pool_size)

            class fake_connector:
                def __init__(self, nodename):
                    self.node = nodename
                    self.stopped = False

                def stopFactory(self):
                    self.stopped = True

            fakes = [fake_connector('fake%02d' % n) for n in range(fake_pending)]
            # by putting them in connectors but not good_conns, these will
            # register as connection-pending
            self.pool.connectors.update(fakes)

            self.assertEqual(self.pool.num_pending_conns(), 2)
            self.pool.adjustPoolSize(pool_size)

            # the pending conns should have been killed first
            self.assertEqual(self.pool.num_pending_conns(), 0)
            self.assertEqual(self.pool.num_connectors(), pool_size)
            self.assertNumConnections(pool_size)
            self.assertNumUniqueConnections(pool_size)

            for fk in fakes:
                self.assert_(fk.stopped, msg='Fake %s was not stopped!' % fk.node)

    @defer.inlineCallbacks
    def test_connection_leveling(self):
        num_nodes = 8
        conns_per_node = 10
        tolerance_factor = 0.20

        def assertConnsPerNode(numconns):
            tolerance = int(tolerance_factor * numconns)
            conns = self.cluster.get_connections()
            pernode = {}
            for node, nodeconns in groupby(sorted(conns), lambda (n,p): n):
                pernode[node] = len(list(nodeconns))
            for node, conns_here in pernode.items():
                self.assertApproximates(numconns, conns_here, tolerance,
                                        msg='Expected ~%r (+- %r) connections to %r,'
                                            ' but found %r. Whole map: %r'
                                            % (numconns, tolerance, node, conns_here,
                                               pernode))

        with self.cluster_and_pool(num_nodes=num_nodes, pool_size=1):
            pool_size = num_nodes * conns_per_node

            yield self.make_standard_cfs()
            yield self.insert_dumb_rows()

            # turn up pool size once other nodes are known
            self.pool.adjustPoolSize(pool_size)
            yield deferwait(0.3)

            # make sure conns are (at least mostly) balanced
            self.assertNumConnections(pool_size)
            self.assertNumUniqueConnections(num_nodes)

            assertConnsPerNode(conns_per_node)

            # kill a node and make sure connections are remade in a
            # balanced way
            node = self.killSomeNode()
            yield deferwait(0.6)

            self.assertNumConnections(pool_size)
            self.assertNumUniqueConnections(num_nodes - 1)

            assertConnsPerNode(pool_size / (num_nodes - 1))

            # lower pool size, check that connections are killed in a
            # balanced way
            new_pool_size = pool_size - conns_per_node
            self.pool.adjustPoolSize(new_pool_size)
            yield deferwait(0.2)

            self.assertNumConnections(new_pool_size)
            self.assertNumUniqueConnections(num_nodes - 1)

            assertConnsPerNode(new_pool_size / (num_nodes - 1))

            # restart the killed node again and wait for the pool to notice
            # that it's up
            node.startService()
            yield deferwait(0.5)

            # raise pool size again, check balanced
            self.pool.adjustPoolSize(pool_size)
            yield deferwait(0.2)

            self.assertNumConnections(pool_size)
            self.assertNumUniqueConnections(num_nodes)

            assertConnsPerNode(conns_per_node)

        self.flushLoggedErrors()

    def test_huge_pool(self):
        pass

    @defer.inlineCallbacks
    def test_manual_node_add(self):
        num_nodes = 3
        pool_size = 5

        class LyingCassanovaNode(cassanova_server.CassanovaNode):
            def endpoint_str(self):
                return '127.0.0.1:%d' % (self.addr.port + 1000)

        class LyingFakeCluster(fake.FakeCassandraCluster):
            node_class = LyingCassanovaNode

        with self.cluster_and_pool(num_nodes=num_nodes, pool_size=1,
                                   cluster_class=LyingFakeCluster):
            yield self.make_standard_cfs()
            yield self.insert_dumb_rows()

            self.pool.conn_timeout = 0.5

            # turn up pool size once other nodes are known
            self.pool.adjustPoolSize(pool_size)
            yield deferwait(0.2)

            # shouldn't have been able to find any nodes besides the seed
            self.assertNumConnections(pool_size)
            self.assertNumUniqueConnections(1)

            # add address for a second real node, raise pool size so new
            # connections are made
            self.pool.addNode((self.cluster.iface, self.cluster.port + 1))
            self.pool.adjustPoolSize(pool_size * 2)
            yield deferwait(0.4)

            self.assertNumConnections(pool_size * 2)
            self.assertNumUniqueConnections(2)

        self.flushLoggedErrors()

    @defer.inlineCallbacks
    def test_manual_node_remove(self):
        num_nodes = 5
        pool_size = 10

        with self.cluster_and_pool(num_nodes=num_nodes, pool_size=1):
            yield self.make_standard_cfs()
            yield self.insert_dumb_rows()

            # turn up pool size once other nodes are known
            self.pool.adjustPoolSize(pool_size)
            yield deferwait(0.2)

            self.assertNumConnections(pool_size)
            self.assertNumUniqueConnections(num_nodes)

            n = iter(self.pool.nodes).next()
            self.pool.removeNode(n)
            yield deferwait(0.2)

            self.assertNumConnections(pool_size)
            self.assertNumUniqueConnections(num_nodes - 1)

            # ask for one extra connection, to make sure the removed node
            # isn't re-added and connected to
            self.pool.adjustPoolSize(pool_size + 1)
            yield deferwait(0.1)

            self.assertNumConnections(pool_size + 1)
            self.assertNumUniqueConnections(num_nodes - 1)

    @defer.inlineCallbacks
    def test_conn_loss_during_idle(self):
        num_nodes = pool_size = 6

        with self.cluster_and_pool(num_nodes=num_nodes, pool_size=1):
            yield self.make_standard_cfs()
            yield self.insert_dumb_rows()

            # turn up pool size once other nodes are known
            self.pool.adjustPoolSize(pool_size)
            yield deferwait(0.2)

            self.assertNumConnections(pool_size)
            self.assertNumUniqueConnections(pool_size)
            self.assertNumWorkers(0)

            self.killSomeConn()
            yield deferwait(0.1)

            self.assertNumConnections(pool_size)
            self.assertNumWorkers(0)

            self.killSomeNode()
            yield deferwait(0.1)

            conns = self.assertNumConnections(pool_size)
            uniqnodes = set(n for (n,p) in conns)
            self.assert_(len(uniqnodes) >= (num_nodes - 1),
                         msg='Expected %d or more unique connected nodes, but found %d'
                             % (num_nodes - 1, len(uniqnodes)))
            self.assertNumWorkers(0)

        self.flushLoggedErrors()

    @defer.inlineCallbacks
    def test_last_conn_loss_during_idle(self):
        with self.cluster_and_pool(pool_size=1, num_nodes=1):
            yield self.make_standard_cfs()
            yield self.insert_dumb_rows()

            no_nodes_called = [False]
            def on_no_nodes(poolsize, targetsize, pendingreqs, expectedwait):
                self.assertEqual(poolsize, 0)
                self.assertEqual(targetsize, 1)
                self.assertEqual(pendingreqs, 0)
                no_nodes_called[0] = True
            self.pool.on_insufficient_nodes = on_no_nodes

            self.assertNumConnections(1)
            node = self.killSomeNode()
            yield deferwait(0.05)

            self.assert_(no_nodes_called[0], msg='on_no_nodes was not called')

            node.startService()
            d = self.pool.get('key004', 'Standard1', '%s-004-007' % self.ksname,
                              retries=2)
            addtimeout(d, 3.0)
            answer = yield d
            self.assertEqual(answer.column.value, 'val-%s-004-007' % self.ksname)

        self.flushLoggedErrors()

    @defer.inlineCallbacks
    def test_last_conn_loss_during_request(self):
        with self.cluster_and_pool(pool_size=1, num_nodes=1):
            yield self.make_standard_cfs()
            yield self.insert_dumb_rows()

            self.assertNumConnections(1)

            d = self.pool.get('key004', 'Standard1/wait=1.0',
                              '%s-004-008' % self.ksname, retries=4)
            yield deferwait(0.1)

            def cancel_if_no_conns(numconns, pending):
                numworkers = self.pool.num_working_conns()
                if numworkers == 0 and not d.called:
                    d.cancel()
            self.pool.on_insufficient_conns = cancel_if_no_conns

            self.assertNumWorkers(1)
            self.killWorkingNode()
            yield deferwait(0.05)

            self.assertFired(d)
            yield self.assertFailure(d, defer.CancelledError)

        self.flushLoggedErrors()

    @defer.inlineCallbacks
    def test_main_seed_down(self):
        with self.cluster_and_pool(pool_size=1, num_nodes=2):
            yield self.make_standard_cfs()
            yield self.insert_dumb_rows(numkeys=20)

            self.pool.adjustPoolSize(5)
            yield deferwait(0.1)
            self.assertNumConnections(5)
            self.assertNumUniqueConnections(2)

            # kill the first seed node
            startnode = [node for (node, proto) in self.cluster.get_connections()
                              if node.addr.port == self.start_port]
            startnode[0].stopService()

            # submit a bunch of read requests
            dlist = []
            keys = yield self.pool.get_range_slices('Standard1', start='',
                                                    count=10, column_count=0)
            for k in keys:
                d = self.pool.get_range_slices('Standard1', start=k.key, finish=k.key,
                                               column_count=10)
                dlist.append(d)

            yield defer.DeferredList(dlist, fireOnOneErrback=True)

        self.flushLoggedErrors()

    @defer.inlineCallbacks
    def test_lots_of_up_and_down(self):
        pool_size = 20
        num_nodes = 10
        num_ops = 500
        num_twiddles = 100
        runtime = 4.0
        ksname = 'KS'
        num_keys = 20

        @defer.inlineCallbacks
        def node_twiddler(optime, numops):
            end_time = time() + optime
            wait_per_op = float(optime) / numops
            log.msg('twiddler starting')
            while True:
                if time() > end_time:
                    break
                yield deferwait(random.normalvariate(wait_per_op, wait_per_op * 0.2))
                nodes = self.cluster.get_nodes()
                running_nodes = [n for n in nodes if n.running]
                nonrunning = [n for n in nodes if not n.running]
                if len(running_nodes) <= 1:
                    op = 'up'
                elif len(nonrunning) == 0:
                    op = 'down'
                else:
                    op = random.choice(('down', 'up'))
                if op == 'down':
                    random.choice(running_nodes).stopService()
                else:
                    random.choice(nonrunning).startService()
            log.msg('twiddler done')

        @defer.inlineCallbacks
        def work_o_tron(optime, numops, n):
            log.msg('work_o_tron %d started' % n)
            end_time = time() + optime
            wait_per_op = float(optime) / numops
            opsdone = 0
            while True:
                if time() > end_time:
                    break
                thiswait = random.normalvariate(wait_per_op, wait_per_op * 0.2)
                keynum = random.randint(0, num_keys - 1)
                log.msg('work_o_tron %d getting key%03d, waiting %f' % (n, keynum, thiswait))
                d = self.pool.get('key%03d' % keynum, 'Standard1/wait=%f' % thiswait,
                                  '%s-%03d-001' % (ksname, keynum),
                                  retries=10)
                result = yield d
                log.msg('work_o_tron %d got %r' % (n, result))
                self.assertEqual(result.column.value, 'val-%s-%03d-001' % (ksname, keynum))
                opsdone += 1
            log.msg('work_o_tron %d done' % n)
            self.assertApproximates(opsdone, numops, 0.5 * numops)

        starttime = time()
        with self.cluster_and_pool(pool_size=1, num_nodes=num_nodes,
                                   api_version=translate.CASSANDRA_08_VERSION):
            yield self.make_standard_cfs(ksname)
            yield self.insert_dumb_rows(ksname, numkeys=num_keys)

            self.pool.adjustPoolSize(pool_size)
            yield deferwait(0.5)

            twiddler = node_twiddler(runtime, num_twiddles)
            workers = [work_o_tron(runtime, num_ops / pool_size, n)
                       for n in range(pool_size)]

            end = yield defer.DeferredList([twiddler] + workers, fireOnOneErrback=True)
            for num, (succ, result) in enumerate(end):
                self.assert_(succ, msg='worker %d failed: result: %s' % (num, result))
        endtime = time()

        self.assertApproximates(endtime - starttime, runtime, 0.5 * runtime)
        self.flushLoggedErrors()

