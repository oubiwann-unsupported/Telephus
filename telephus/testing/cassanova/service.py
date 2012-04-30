import os

from twisted.application import service

from telephus.cassandra.c08 import ttypes
from telephus.testing.cassanova import base, partitioner, server, types


cluster_name = os.environ.get('CASSANOVA_CLUSTER_NAME', 'Fake Cluster')


class CassanovaService(service.MultiService):
    """
    """
    partitioner = partitioner.RandomPartitioner()
    snitch = base.SimpleSnitch()
    port = 9160

    def __init__(self, port=None):
        service.MultiService.__init__(self)
        self.keyspaces = {'system': ttypes.KsDef(
            'system',
            replication_factor=1,
            strategy_class='org.apache.cassandra.locator.LocalStrategy',
            cf_defs=[]
        )}
        self.data = {}
        if port is not None:
            self.port = port
        self.ring = {}
        self.clustername = cluster_name

    def add_node(self, iface, token=None):
        node = server.CassanovaNode(self.port, iface, token=token)
        node.setServiceParent(self)
        self.ring[node.mytoken] = node

    def get_keyspace(self, ks):
        try:
            return self.keyspaces[ks]
        except KeyError:
            raise ttypes.InvalidRequestException(
                why='No such keyspace %r' % ks)

    def get_range_to_endpoint_map(self):
        ring = sorted(self.ring.items())
        trmap = {}
        for num, (tok, node) in enumerate(ring):
            endpoints = (ring[num-1][0], tok)
            trmap[tuple(map(
                self.partitioner.token_as_bytes, endpoints))] = [node]
        return trmap

    def get_schema(self):
        return self.keyspaces.values()

    def lookup_cf(self, ks, cfname):
        ksdef = self.get_keyspace(ks.name)
        for cfdef in ksdef.cf_defs:
            if cfdef.name == cfname:
                break
        else:
            raise ttypes.InvalidRequestException(
                why='No such column family %r' % cfname)
        return cfdef

    def lookup_cf_and_data(self, ks, cfname):
        cfdef = self.lookup_cf(ks, cfname)
        datadict = self.data.setdefault(
            ks.name, {}).setdefault(cfname, {None: cfname})
        return cfdef, datadict

    def lookup_cf_and_row(self, ks, key, cfname, make=False):
        cf, data = self.lookup_cf_and_data(ks, cfname)
        try:
            row = data[key]
        except KeyError:
            if make:
                row = data[key] = {None: key}
            else:
                raise
        return cf, row

    def lookup_column_parent(self, ks, key, column_parent, make=False):
        cf, row = self.lookup_cf_and_row(
            ks, key, column_parent.column_family, make=make)
        if cf.column_type == 'Super':
            if column_parent.super_column is None:
                colparent = row
            else:
                try:
                    colparent = row[column_parent.super_column]
                except KeyError:
                    if make:
                        sc = column_parent.super_column
                        colparent = row[sc] = {None: sc}
                    else:
                        raise
        else:
            if column_parent.super_column is not None:
                raise ttypes.InvalidRequestException(
                    why='Improper ColumnParent to ColumnFamily')
            colparent = row
        return colparent

    def lookup_column_path(self, ks, key, column_path):
        try:
            colparent = self.lookup_column_parent(ks, key, column_path)
        except ttypes.InvalidRequestException, e:
            newmsg = e.why.replace('ColumnParent', 'ColumnPath')
            raise ttypes.InvalidRequestException(why=newmsg)
        if column_path.column is None:
            return colparent
        return colparent[column_path.column]

    def comparator_for(self, ks, column_parent):
        cf = self.lookup_cf(ks, column_parent.column_family)
        if column_parent.super_column is not None:
            return self.load_class_by_java_name(
                cf.subcomparator_type).comparator
        else:
            return self.load_class_by_java_name(cf.comparator_type).comparator

    def key_comparator(self):
        return self.partitioner.key_cmp

    def load_class_by_java_name(self, name):
        return base.java_class_map[name]

    def slice_predicate(self, comparator, slicerange):
        start = slicerange.start
        end = slicerange.finish
        if slicerange.reversed:
            start, end = end, start
        if start == self.partitioner.min_bound:
            scmp = lambda n: True
        else:
            scmp = lambda n: comparator(start,n) <= 0
        if end == self.partitioner.min_bound:
            ecmp = lambda n: True
        else:
            ecmp = lambda n: comparator(end,n) >= 0
        return lambda n: scmp(n) and ecmp(n)

    def key_predicate_keys(self, compar, start, end):
        if end == self.partitioner.min_bound:
            return lambda k: compar(k, start) >= 0
        else:
            return lambda k: compar(k, start) >= 0 \
                             and compar(k, end) <= 0

    def key_predicate_tokens(self, compar, start, end):
        if compar(start, end) >= 0:
            return lambda k: compar(k, start) > 0 \
                             or compar(k, end) <= 0
        else:
            return lambda k: compar(k, start) > 0 \
                             and compar(k, end) <= 0

    def key_predicate(self, comparator, keyrange):
        isnones = [getattr(keyrange, x) is not None
                   for x in ('start_key', 'end_key', 'start_token', 
                             'end_token')]
        if isnones == [True, True, False, False]:
            return self.key_predicate_keys(
                comparator, keyrange.start_key, keyrange.end_key)
        elif isnones == [False, False, True, True]:
            return self.key_predicate_tokens(
                comparator, keyrange.start_token, keyrange.end_token)
        raise ttypes.InvalidRequestException(why="Improper KeyRange")

    def get_nodes(self):
        return self.ring.values()

    def get_connections(self):
        """
        Get a list of (CassanovaNode, CassanovaServerProtocol) tuples
        corresponding to all current incoming connections. This can be useful
        in testing client's handling of connection failures; call
        proto.transport.loseConnection() to force a loss.
        """

        conns = []
        nodes = sorted(self.get_nodes())
        for n in nodes:
            conns.extend((n, proto) for proto in n.conns_to_me)
        return conns

    def get_working_connections(self):
        workers = []
        for node, proto in self.get_connections():
            if proto.working:
                workers.append((node, proto))
        return workers
