from itertools import izip, groupby
import socket
import sys

from twisted.application import service
from twisted.internet import defer, error
from twisted.python import failure, log

from thrift import Thrift
from thrift.transport import TTransport

from telephus import exceptions
from telephus.cassandra.c08 import ttypes
from telephus.client import CassandraClient
from telephus.pool.connection import (
    CassandraKeyspaceConnection, lame_log_insufficient_nodes)
from telephus.pool.protocol import CassandraPoolReconnectorFactory
from telephus.pool.server import CassandraNode
from telephus.protocol import ManagedThriftRequest


noop = lambda *a, **kw: None


class CassandraClusterPool(service.Service, object):
    """
    Manage a pool of connections to nodes in a Cassandra cluster.

    Requests made to the pool will go to whichever host is the least loaded (as
    far as this class can tell). If the requests specify multiple retries, the
    retries will be executed on different hosts if possible.

    Will periodically check an unparticular connection to see if new nodes can
    be found, and add them to the pool.

    Note that like most Services, the pool will not start until startService is
    called. If you have a parent Service (like a
    L{twisted.service.application.Application} instance), set that to be this
    service's parent:

        >>> cluster_pool.setServiceParent(application)

    and the startService() and stopService() methods will be called when
    appropriate.

    @ivar default_cassandra_thrift_port: just what it says on the tin

    @ivar max_connections_per_node: do our best not not to exceed this many
        connections to a single Cassandra endpoint

    @type max_connections_per_node: int

    @ivar on_insufficient_nodes: if set to a callback, this will be called
        in the event that there are no valid places to connect to expand
        the pool to its target size. Regardless of actions taken by this
        callback, the service will wait until a node is expected to be
        available and then check again.

    @type on_insufficient_nodes: callback taking four arguments: the current
        size of the connection pool, the target size of the pool, the
        number of pending requests, and the number of seconds before a
        candidate node will be available to try connecting (or None, if no
        candidate is in sight).

    @ivar on_insufficient_conns: if set to a callback, this will be called
        when a request is made and all current connections are busy. The
        request will still be expected to go through, once another connection
        is available, but it may be helpful to know how often this is
        happening and possibly expand the pool

    @type on_insufficient_conns: callback taking two arguments: the current
        size of the connection pool, and the number of requests which are
        pending in the CassandraClusterPool queue

    @ivar request_retries: the default number of retries which will be
        performed for requests when the retry number is unspecified

    @type request_retries: int

    @ivar retryables: A list of Exception types which, if they are raised in
        the course of a Cassandra Thrift operation, mean both that (a) the
        request can be tried again on another connection, and that (b) if the
        connection was lost right after this error, it can be retried
        immediately
    """

    default_cassandra_thrift_port = 9160
    max_connections_per_node = 25
    on_insufficient_nodes = staticmethod(lame_log_insufficient_nodes)
    on_insufficient_conns = staticmethod(noop)
    request_retries = 4
    conn_factory = CassandraPoolReconnectorFactory

    retryables = (IOError, socket.error, Thrift.TException,
                  ttypes.TimedOutException, ttypes.UnavailableException,
                  TTransport.TTransportException)

    def __init__(self, seed_list, keyspace=None, creds=None, thrift_port=None,
                 pool_size=None, conn_timeout=10, bind_address=None,
                 log_cb=log.msg, reactor=None, api_version=None):
        """
        Initialize a CassandraClusterPool.

        @param keyspace: If given and not None, determines the keyspace to
            which all connections in this pool will be made.

        @param creds: Credentials to use to authenticate Cassandra connections

        @type creds: A dict (or other mapping) of strings to strings

        @param seed_list: An initial set of host addresses which, if
            connectable, are part of this cluster.

        @type seed_list: iterable

        @param thrift_port: The port to use for connections to Cassandra nodes

        @param pool_size: The target size for the connection pool. Naturally,
            the actual size may be higher or lower as nodes connect and
            disconnect, but an effort will be made to adjust toward this size.

        @type pool_size: int

        @param conn_timeout: The number of seconds before a pending connection
            is deemed unsuccessful and aborted. Of course, when a connection
            error can be detected before this time, the connection will be
            aborted appropriately.

        @type conn_timeout: float

        @param bind_address: The local interface to which to bind when making
            outbound connections. Default: determined by the system's socket
            layer.

        @type bind_address: str

        @param log_cb: A callable which is expected to work like
            L{twisted.python.log.msg}. Will be used when certain connection
            and disconnection events occur. The default is log.msg.

        @param reactor: The reactor instance to use when starting thrift
            connections or setting timers.

        @param api_version: If not None, Telephus will require that
            all connections conform to the API for the given Cassandra version.
            Possible values are "0.7", "0.8", "1.0", etc.

            If None, Telephus will consider all supported API versions to be
            acceptable.

            If the api version reported by a remote node is not compatible, the
            connection to that node will be aborted. Default: None
        """

        self.seed_list = list(seed_list)
        if thrift_port is None:
            thrift_port = self.default_cassandra_thrift_port
        self.thrift_port = thrift_port
        if pool_size is None:
            pool_size = len(self.seed_list)
        self.target_pool_size = pool_size
        self.log = log_cb
        self.conn_timeout = conn_timeout
        self.bind_address = bind_address
        self.keyspace = keyspace
        self.creds = creds
        self.request_queue = defer.DeferredQueue()
        self.api_version = api_version
        self.future_fill_pool = None
        self.removed_nodes = set()
        self._client_instance = CassandraClient(self)

        if reactor is None:
            from twisted.internet import reactor
        self.reactor = reactor

        # A set of CassandraNode instances representing known nodes. This
        # includes nodes from the initial seed list, nodes seen in
        # describe_ring calls to existing nodes, and nodes explicitly added
        # by the addNode() method. Nodes are only removed from this set if
        # no connections have been successful in self.forget_node_interval
        # seconds, or by an explicit call to removeNode().
        self.nodes = set()

        # A set of CassandraPoolReconnectorFactory instances corresponding to
        # connections which are either live or pending. Failed attempts to
        # connect will remove a connector from this set. When connections are
        # lost, an immediate reconnect will be attempted.
        self.connectors = set()

        # A collection of objects from self.connectors corresponding to
        # existing, working (as far as we know) connections. This will be
        # derivable from self.connectors, but hopefully will be maintained to
        # present a good snapshot of what is alive, now, and what is not.
        # This is stored in a deque so that it can be efficiently rotated
        # to distribute requests.
        self.good_conns = set()

        # A set of CassandraPoolReconnectorFactory instances, formerly in
        # self.connectors, the connections for which are draining. No new
        # requests should be fed to these instances; they are tracked only so
        # that they can be terminated more fully in case this service is shut
        # down before they finish.
        self.dying_conns = set()

    def startService(self):
        service.Service.startService(self)
        for addr in self.seed_list:
            if isinstance(addr, tuple) and len(addr) == 2:
                self.addNode(addr)
            else:
                self.addNode((addr, self.thrift_port))
        self.fill_pool()

    def stopService(self):
        service.Service.stopService(self)
        if (self.future_fill_pool is not None and
            self.future_fill_pool.active()):
            self.future_fill_pool.cancel()
        for factory in self.connectors.copy():
            factory.service = None
            factory.stopFactory()
        self.connectors = set()
        self.good_conns = set()
        self.dying_conns = set()

    def addNode(self, node):
        if not isinstance(node, CassandraNode):
            node = CassandraNode(*node)
        if node in self.nodes:
            raise ValueError("%s is already known" % (node,))
        if node in self.removed_nodes:
            self.removed_nodes.remove(node)
        self.nodes.add(node)

    def removeNode(self, node):
        if not isinstance(node, CassandraNode):
            node = CassandraNode(*node)
        for f in self.all_connectors_to(node):
            f.stopFactory()
            self.remove_connector(f)
        for f in self.dying_conns.copy():
            if f.node == node:
                f.stopFactory()
                self.remove_connector(f)
        self.removed_nodes.add(node)
        self.nodes.remove(node)
        self.fill_pool()

    def err(self, _stuff=None, _why=None, **kw):
        if _stuff is None:
            _stuff = failure.Failure()
        kw['isError'] = True
        kw['why'] = _why
        if isinstance(_stuff, failure.Failure):
            self.log(failure=_stuff, **kw)
        elif isinstance(_stuff, Exception):
            self.log(failure=failure.Failure(_stuff), **kw)
        else:
            self.log(repr(_stuff), **kw)

    # methods for inspecting current connection state
    def all_connectors(self):
        return self.connectors.copy()

    def num_connectors(self):
        """
        Return the total number of current connectors, including both live and
        pending connections.
        """
        return len(self.connectors)

    def all_connectors_to(self, node):
        return [f for f in self.connectors if f.node == node]

    def num_connectors_to(self, host):
        return len(self.all_connectors_to(host))

    def all_active_conns(self):
        return self.good_conns.copy()

    def num_active_conns(self):
        return len(self.good_conns)

    def all_active_conns_to(self, node):
        return [f for f in self.good_conns if f.node == node]

    def num_active_conns_to(self, node):
        return len(self.all_active_conns_to(node))

    def all_working_conns(self):
        return [f for f in self.good_conns if f.jobphase == 'pending_request']

    def num_working_conns(self):
        return len(self.all_working_conns())

    def all_pending_conns(self):
        return self.connectors - self.good_conns

    def num_pending_conns(self):
        return len(self.all_pending_conns())

    def all_pending_conns_to(self, node):
        return [f for f in self.all_pending_conns() if f.node == node]

    def num_pending_conns_to(self, node):
        return len(self.all_pending_conns_to(node))

    def add_connection_score(self, node):
        """
        Return a numeric value that determines this node's score for adding
        a new connection. A negative value indicates that no connections
        should be made to this node for at least that number of seconds.
        A value of -inf indicates no connections should be made to this
        node for the foreseeable future.

        This score should ideally take into account the connectedness of
        available nodes, so that those with less current connections will
        get more.
        """

        # TODO: this should ideally take node history into account

        conntime = node.seconds_until_connect_ok()
        if conntime > 0:
            self.log("not considering %r for new connection; has %r left on "
                     "connect blackout" % (node, conntime))
            return -conntime
        numconns = self.num_connectors_to(node)
        if numconns >= self.max_connections_per_node:
            return float('-Inf')
        return sys.maxint - numconns

    def adjustPoolSize(self, newsize):
        """
        Change the target pool size. If we have too many connections already,
        ask some to finish what they're doing and die (preferring to kill
        connections to the node that already has the most connections). If
        we have too few, create more.
        """

        if newsize < 0:
            raise ValueError("pool size must be nonnegative")
        self.log("Adjust pool size from %d to %d." % (
            self.target_pool_size, newsize))
        self.target_pool_size = newsize
        self.kill_excess_pending_conns()
        self.kill_excess_conns()
        self.fill_pool()

    def update_known_nodes(self, ring):
        for tokenrange in ring:
            for addr in tokenrange.endpoints:
                if ':' in addr:
                    addr, port = addr.split(':', 1)
                    port = int(port)
                else:
                    port = self.thrift_port
                node = CassandraNode(addr, port)
                if node not in self.removed_nodes and node not in self.nodes:
                    self.addNode(node)

    def choose_nodes_to_connect(self):
        while True:
            nodes = list(self.nodes)
            scores = map(self.add_connection_score, nodes)
            bestscore, bestnode = max(zip(scores, nodes))
            if bestscore < 0:
                raise exceptions.NoNodesAvailable(-bestscore)
            yield bestnode

    def choose_pending_conns_to_kill(self):
        # prefer to junk pending conns to most-redundantly-connected node
        while True:
            pending_conns = self.all_pending_conns()
            if len(pending_conns) == 0:
                break
            yield max(
                pending_conns, key=lambda f: self.num_connectors_to(f.node))

    def choose_conns_to_kill(self):
        nodegetter = lambda f: f.node
        # prefer to junk conns to most-redundantly-connected node
        while True:
            active_conns = self.all_active_conns()
            if len(active_conns) == 0:
                break
            nodes_and_conns = groupby(
                sorted(active_conns, key=nodegetter), nodegetter)
            nodes_and_counts = (
                (n, len(list(conns))) for (n, conns) in nodes_and_conns)
            bestnode, bestcount = max(
                nodes_and_counts, key=lambda (n, count): count)
            # should be safe from IndexError
            yield self.all_active_conns_to(bestnode)[0]

    def kill_excess_pending_conns(self):
        killnum = self.num_connectors() - self.target_pool_size
        if killnum <= 0:
            return
        for n, f in izip(xrange(killnum), self.choose_pending_conns_to_kill()):
            self.log("Aborting pending conn to %r" % (f.node,))
            f.stopFactory()
            self.remove_connector(f)

    def kill_excess_conns(self):
        need_to_kill = self.num_active_conns() - self.target_pool_size
        if need_to_kill <= 0:
            return
        for n, f in izip(xrange(need_to_kill), self.choose_conns_to_kill()):
            self.log("Draining conn to %r" % (f.node,))
            f.finish_and_die()
            self.remove_connector(f)
            self.dying_conns.add(f)

    def fill_pool(self):
        """
        Add connections as necessary to meet the target pool size. If there
        are no nodes to connect to (because we maxed out connections-per-node
        on all active connections and any unconnected nodes have pending
        reconnect timers), call the on_insufficient_nodes callback.
        """

        need = self.target_pool_size - self.num_connectors()
        if need <= 0:
            return
        try:
            for num, node in izip(
                xrange(need), self.choose_nodes_to_connect()):
                self.make_conn(node)
        except exceptions.NoNodesAvailable, e:
            waittime = e.args[0]
            if waittime == float('Inf'):
                waittime = None
            pending_requests = len(self.request_queue.pending)
            if self.on_insufficient_nodes:
                self.on_insufficient_nodes(
                    self.num_active_conns(), self.target_pool_size,
                    pending_requests, waittime)
            self.schedule_future_fill_pool(e.args[0])
            if self.num_connectors() == 0 and pending_requests > 0:
                if self.on_insufficient_conns:
                    self.on_insufficient_conns(self.num_connectors(),
                                               pending_requests)

    def schedule_future_fill_pool(self, seconds):
        if seconds == float('Inf'):
            return
        if self.future_fill_pool is None or not self.future_fill_pool.active():
            self.future_fill_pool = self.reactor.callLater(
                seconds, self.fill_pool)
        else:
            self.future_fill_pool.reset(seconds)

    def make_conn(self, node):
        self.log('Adding connection to %s' % (node,))
        f = self.conn_factory(node, self, self.api_version)
        bindaddr = self.bind_address
        if bindaddr is not None and isinstance(bindaddr, str):
            bindaddr = (bindaddr, 0)
        self.reactor.connectTCP(node.host, node.port, f,
                                timeout=self.conn_timeout,
                                bindAddress=bindaddr)
        self.connectors.add(f)

    def remove_good_conn(self, f):
        try:
            self.good_conns.remove(f)
        except KeyError:
            pass

    def remove_connector(self, f):
        self.remove_good_conn(f)
        try:
            self.connectors.remove(f)
        except KeyError:
            try:
                self.dying_conns.remove(f)
            except KeyError:
                pass

    def client_conn_failed(self, reason, f):
        is_notable = f.node.conn_fail(reason)
        f.stopFactory()
        self.remove_connector(f)
        if is_notable:
            self.err(reason, 'Thrift pool connection to %s failed' % (f.node,))
        self.fill_pool()

    def client_conn_made(self, f):
        d = f.prep_connection(self.creds, self.keyspace)
        d.addCallback(self.client_ready, f)
        d.addErrback(self.client_conn_failed, f)

    def client_ready(self, ring, f):
        self.update_known_nodes(ring)
        f.node.conn_success()
        self.good_conns.add(f)
        self.log('Successfully added connection to %s to the pool' % (f.node,))
        f.work_on_queue(self.request_queue)

    def client_conn_lost(self, f, reason):
        if reason.check(error.ConnectionDone):
            self.log(
                'Thrift pool connection to %s failed (cleanly)' % (f.node,))
        else:
            self.err(
                reason, 'Thrift pool connection to %s was lost' % (f.node,))
        if f.last_error is None or f.last_error.check(*self.retryables):
            self.log('Retrying connection right away')
            self.remove_good_conn(f)
            f.retry()
        else:
            f.node.conn_fail(reason)
            f.stopFactory()
            self.remove_connector(f)
            self.fill_pool()

    def pushRequest(self, req, retries=None, keyspace=None):
        if keyspace is None:
            keyspace = self.keyspace
        retries = retries if retries is not None else self.request_retries
        req_d = defer.Deferred()
        self.pushRequest_really(req, keyspace, req_d, retries)
        return req_d

    def pushRequest_really(self, req, keyspace, req_d, retries):
        call_insuff_conns = False
        if len(self.request_queue.waiting) == 0:
            # no workers are immediately available
            if self.on_insufficient_conns:
                call_insuff_conns = True
        self.request_queue.put((req, keyspace, req_d, retries))
        if call_insuff_conns:
            self.on_insufficient_conns(self.num_connectors(),
                                       len(self.request_queue.pending))

    def resubmit(self, req, keyspace, req_d, retries):
        """
        Push this request to the front of the line, just to be a jerk.
        """
        self.log('resubmitting %s request' % (req.method,))
        self.pushRequest_really(req, keyspace, req_d, retries)
        try:
            self.request_queue.pending.remove((req, keyspace, req_d, retries))
        except ValueError:
            # it's already been scooped up
            pass
        else:
            self.request_queue.pending.insert(
                0, (req, keyspace, req_d, retries))

    def set_keyspace(self, keyspace):
        """
        Change the keyspace which will be used for subsequent requests to this
        CassandraClusterPool, and return a Deferred that will fire once it can
        be verified that connections can successfully use that keyspace.

        If something goes wrong trying to change a connection to that keyspace,
        the Deferred will errback, and the keyspace to be used for future
        requests will not be changed.

        Requests made between the time this method is called and the time that
        the returned Deferred is fired may be made in either the previous
        keyspace or the new keyspace. If you may need to make use of multiple
        keyspaces at the same time in the same app, consider using the
        specialized CassandraKeyspaceConnection interface provided by the
        keyspaceConnection method.
        """

        # push a real set_keyspace on some (any) connection; the idea is that
        # if it succeeds there, it is likely to succeed everywhere, and vice
        # versa.  don't bother waiting for all connections to change- some of
        # them may be doing long blocking tasks and by the time they're done,
        # the keyspace might be changed again anyway
        d = self.pushRequest(ManagedThriftRequest('set_keyspace', keyspace))

        def store_keyspace(_):
            self.keyspace = keyspace
        d.addCallback(store_keyspace)
        return d

    def __getattr__(self, name):
        """
        Make CassandraClusterPool act like its own CassandraClient when
        the user wants to use it that way
        """
        return getattr(self._client_instance, name)

    def get_consistency(self):
        return self._client_instance.consistency

    def set_consistency(self, value):
        self._client_instance.consistency = value

    consistency = property(get_consistency, set_consistency)

    def keyspaceConnection(self, keyspace, 
                           consistency=ttypes.ConsistencyLevel.ONE):
        """
        Return a CassandraClient instance which uses this CassandraClusterPool
        by way of a CassandraKeyspaceConnection, so that all requests made
        through it are guaranteed to go to the given keyspace, no matter what
        other consumers of this pool may do.
        """
        return CassandraClient(CassandraKeyspaceConnection(self, keyspace),
                               consistency=consistency)

    def __str__(self):
        return '<%s: [%d nodes known] [%d connections]>' \
               % (self.__class__.__name__, len(self.nodes),
                  self.num_active_conns())

    __repr__ = __str__
