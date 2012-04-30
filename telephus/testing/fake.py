from twisted.internet import defer

from telephus.cassandra import ttypes
from telephus.testing.util import deferwait
from telephus.testing.cassanova import (
    api as cassanova_api, server as cassanova_server,
    service as cassanova_service)


class EnhancedCassanova(cassanova_api.Cassanova):
    """
    Add a way to request operations which are guaranteed to take (at least) a
    given amount of time, for easier testing of things which might take a long
    time in the real world
    """

    def get(self, key, column_path, consistency_level):
        args = []
        if '/' in column_path.column_family:
            parts = column_path.column_family.split('/')
            column_path.column_family = parts[0]
            args = parts[1:]
        d = defer.maybeDeferred(cassanova_api.Cassanova.get, self, key,
                                column_path, consistency_level)
        waittime = 0
        for arg in args:
            if arg.startswith('wait='):
                waittime += float(arg[5:])
        if waittime > 0:
            def doWait(x):
                waiter = deferwait(waittime, x)
                self.service.waiters.append(waiter)
                return waiter
            d.addCallback(doWait)
        return d


class EnhancedCassanovaFactory(cassanova_server.CassanovaFactory):
    handler_factory = EnhancedCassanova


class EnhancedCassanovaNode(cassanova_server.CassanovaNode):
    factory = EnhancedCassanovaFactory

    def endpoint_str(self):
        return '%s:%d' % (self.addr.host, self.addr.port)


class FakeCassandraCluster(cassanova_service.CassanovaService):
    """
    Tweak the standard Cassanova service to allow nodes to run on the same
    interface, but different ports. CassandraClusterPool already knows how
    to understand the 'host:port' type of endpoint description in
    describe_ring output.
    """

    node_class = EnhancedCassanovaNode

    def __init__(self, num_nodes, start_port=41356, interface='127.0.0.1'):
        cassanova_service.CassanovaService.__init__(self, start_port)
        self.waiters = []
        self.iface = interface
        for n in range(num_nodes):
            self.add_node_on_port(start_port + n)
        # make a non-system keyspace so that describe_ring can work
        self.keyspaces['dummy'] = ttypes.KsDef(
            'dummy',
            replication_factor=1,
            strategy_class='org.apache.cassandra.locator.SimpleStrategy',
            cf_defs=[]
        )

    def add_node_on_port(self, port, token=None):
        node = self.node_class(port, self.iface, token=token)
        node.setServiceParent(self)
        self.ring[node.mytoken] = node

    def stopService(self):
        cassanova_service.CassanovaService.stopService(self)
        for d in self.waiters:
            if not d.called:
                d.cancel()
                d.addErrback(lambda n: None)
