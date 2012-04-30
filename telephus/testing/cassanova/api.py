from functools import partial
import re

from zope.interface import implements

from twisted.python import log

from telephus.cassandra.c08 import Cassandra, constants
from telephus.testing.cassanova import types


valid_ks_name_re = re.compile(r'^[a-z][a-z0-9_]*$', re.I)
valid_cf_name_re = re.compile(r'^[a-z][a-z0-9_]*$', re.I)
identity = lambda n: n


class Cassanova(object):
    """
    """
    implements(Cassandra.Iface)

    def __init__(self, node):
        self.node = node
        self.service = node.parent
        self.keyspace = None

    def login(self, auth_request):
        pass

    def set_keyspace(self, keyspace):
        k = self.service.get_keyspace(keyspace)
        self.keyspace = k

    def get(self, key, column_path, consistency_level):
        try:
            val = self.service.lookup_column_path(
                self.keyspace, key, column_path)
        except KeyError, e:
            # should get throw NFE when the cf doesn't even exist? or IRE
            # still?
            log.msg('Could not find %r (keyspace %r)' % (
                e.args[0], self.keyspace.name))
            raise types.NotFoundException()
        if isinstance(val, dict):
            sc = self.pack_up_supercolumn(self.keyspace, column_path, val)
            val = types.ColumnOrSuperColumn(super_column=sc)
        else:
            val = types.ColumnOrSuperColumn(column=val)
        return val

    def get_slice(self, key, column_parent, predicate, consistency_level):
        try:
            cols = self.service.lookup_column_parent(
                self.keyspace, key, column_parent)
        except KeyError:
            return []
        filteredcols = self.filter_by_predicate(column_parent, cols, predicate)
        if len(filteredcols) == 0:
            return filteredcols
        if isinstance(filteredcols[0], dict):
            pack = partial(
                self.pack_up_supercolumn, self.keyspace, column_parent)
            typ = 'super_column'
        else:
            pack = identity
            typ = 'column'
        return [types.ColumnOrSuperColumn(**{typ: pack(col)})
                for col in filteredcols]

    def get_count(self, key, column_parent, predicate, consistency_level):
        return len(self.get_slice(
            key, column_parent, predicate, consistency_level))

    def multiget_slice(self, keys, column_parent, predicate,
                       consistency_level):
        result = {}
        for k in keys:
            cols = self.get_slice(
                k, column_parent, predicate, consistency_level)
            if cols:
                result[k] = cols
        return result

    def multiget_count(self, keys, column_parent, predicate,
                       consistency_level):
        slices = self.multiget_slice(
            keys, column_parent, predicate, consistency_level)
        return dict((k, len(cols)) for (k, cols) in slices.iteritems())

    def get_range_slices(self, column_parent, predicate, keyrange,
                         consistency_level):
        cf, dat = self.service.lookup_cf_and_data(self.keyspace,
                                                  column_parent.column_family)
        compar = self.service.key_comparator()
        keypred = self.service.key_predicate(compar, keyrange)
        count = keyrange.count
        keys = sorted((k for k in dat.iterkeys() if k is not None), cmp=compar)
        out = []
        for k in keys:
            if keypred(k):
                cols = self.get_slice(
                    k, column_parent, predicate, consistency_level)
                out.append(types.KeySlice(key=k, columns=cols))
                if len(out) >= count:
                    break
        return out

    def get_indexed_slices(self, column_parent, index_clause, column_predicate,
                           consistency_level):
        raise types.InvalidRequestException(
            why='get_indexed_slices unsupported here')

    def insert(self, key, column_parent, column, consistency_level):
        if not 0 < len(column.name) < (1<<16):
            raise types.InvalidRequestException('invalid column name')
        try:
            parent = self.service.lookup_column_parent(
                self.keyspace, key, column_parent, make=True)
        except KeyError, e:
            raise types.InvalidRequestException(why=e.args[0])
        parent[column.name] = column

    def remove_column(self, key, column_path, tstamp, consistency_level):
        try:
            parent = self.service.lookup_column_parent(
                self.keyspace, key, column_path)
        except KeyError, e:
            raise types.InvalidRequestException(
                why='column parent %s not found' % e.args[0])
        try:
            col = parent[column_path.column]
            if col.timestamp <= tstamp:
                del parent[column_path.column]
        except KeyError:
            pass

    def remove_cols_from(self, row, tstamp):
        newrow = {}
        for colname, col in row.iteritems():
            if colname is not None and col.timestamp <= tstamp:
                continue
            newrow[colname] = col
        return newrow

    def remove_super_column(self, key, column_parent, tstamp,
                            consistency_level):
        try:
            _, row = self.service.lookup_cf_and_row(
                self.keyspace, key, column_parent.column_family)
            oldsc = row[column_parent.super_column]
        except KeyError:
            return
        newsc = self.remove_cols_from(oldsc, tstamp)
        if len(newsc) <= 1:
            del row[column_parent.super_column]
        else:
            row[column_parent.super_column] = newsc

    def remove_key(self, cfname, key, tstamp, consistency_level):
        try:
            cf, data = self.service.lookup_cf_and_data(self.keyspace, cfname)
            row = data[key]
        except KeyError:
            return
        if cf.column_type == 'Super':
            par = types.ColumnParent(column_family=cfname)
            for scname in list(row.keys()):
                if scname is None:
                    continue
                par.super_column = scname
                self.remove_super_column(key, par, tstamp, consistency_level)
        else:
            row = self.remove_cols_from(row, tstamp)
        data[key] = row

    def remove(self, key, column_path, tstamp, consistency_level):
        if column_path.column is not None:
            return self.remove_column(
                key, column_path, tstamp, consistency_level)
        else:
            if column_path.super_column is not None:
                return self.remove_super_column(
                    key, column_path, tstamp, consistency_level)
            else:
                return self.remove_key(
                    column_path.column_family, key, tstamp, consistency_level)

    def apply_mutate_delete(self, cf, row, deletion, consistency_level):
        cp = types.ColumnPath(
            column_family=cf.name, super_column=deletion.super_column)
        if deletion.predicate is None:
            return self.remove(
                row[None], cp, deletion.timestamp, consistency_level)
        if deletion.predicate.slice_range is not None:
            raise types.InvalidRequestException(
                'Not supposed to support batch_mutate deletions with a '
                'slice_range (although we would if this check was gone)')
        if deletion.super_column is not None:
            try:
                row = row[deletion.super_column]
            except KeyError:
                return
        killme = self.filter_by_predicate(cp, row, deletion.predicate)
        for col in killme:
            if isinstance(col, dict):
                del row[col[None]]
            else:
                del row[col.name]

    def apply_mutate_insert_super(self, row, sc, consistency_level):
        scdat = {None: sc.name}
        for col in sc.columns:
            scdat[col.name] = col
        row[sc.name] = scdat

    def apply_mutate_insert_standard(self, row, col, consistency_level):
        row[col.name] = col

    def apply_mutate_insert(self, cf, row, cosc, consistency_level):
        if cf.column_type == 'Super':
            if cosc.super_column is None:
                raise types.InvalidRequestException(
                    'inserting Column into SuperColumnFamily')
            self.apply_mutate_insert_super(
                row, cosc.super_column, consistency_level)
        else:
            if cosc.super_column is not None:
                raise types.InvalidRequestException(
                    'inserting SuperColumn into standard ColumnFamily')
            self.apply_mutate_insert_standard(
                row, cosc.column, consistency_level)

    def batch_mutate(self, mutation_map, consistency_level):
        for key, col_mutate_map in mutation_map.iteritems():
            for cfname, mutatelist in col_mutate_map.iteritems():
                cf, dat = self.service.lookup_cf_and_row(
                    self.keyspace, key, cfname, make=True)
                for mutation in mutatelist:
                    if (mutation.column_or_supercolumn is not None
                    and mutation.deletion is not None):
                        raise types.InvalidRequestException(
                            why='Both deletion and insertion in same mutation')
                    if mutation.column_or_supercolumn is not None:
                        self.apply_mutate_insert(
                            cf, dat, mutation.column_or_supercolumn,
                            consistency_level)
                    else:
                        self.apply_mutate_delete(
                            cf, dat, mutation.deletion, consistency_level)

    def truncate(self, cfname):
        try:
            cf, dat = self.service.lookup_cf_and_data(self.keyspace, cfname)
        except KeyError, e:
            raise types.InvalidRequestException(why=e.args[0])
        name = dat[None]
        dat.clear()
        dat[None] = name

    def describe_schema_versions(self):
        smap = {}
        for c in self.service.ring.values():
            smap.setdefault(c.schema_code(), []).append(c.addr.host)
        return smap

    def describe_keyspaces(self):
        return self.service.get_schema()

    def describe_cluster_name(self):
        return self.service.clustername

    def describe_version(self):
        return constants.VERSION

    def describe_ring(self, keyspace):
        if keyspace == 'system':
            # we are a bunch of jerks
            raise types.InvalidRequestException(
                "no replication ring for keyspace 'system'")
        self.service.get_keyspace(keyspace)
        trmap = self.service.get_range_to_endpoint_map()
        return [types.TokenRange(
            start_token=lo, end_token=hi, endpoints=[c.endpoint_str()
                for c in endpoints])
            for ((lo, hi), endpoints) in trmap.items()]

    def describe_partitioner(self):
        return self.service.partitioner.java_name

    def describe_snitch(self):
        return self.service.snitch.java_name

    def describe_keyspace(self, ksname):
        try:
            return self.service.get_keyspace(ksname)
        except types.InvalidRequestException:
            raise types.NotFoundException()

    def describe_splits(self, cfname, starttoken, endtoken, keys_per_split):
        raise types.InvalidRequestException(
            why='describe_splits unsupported here')

    def system_add_column_family(self, cfdef):
        ks = self.keyspace
        for cf in ks.cf_defs:
            if cf.name == cfdef.name:
                raise types.InvalidRequestException(
                    why='CF %r already exists' % cf.name)
        self.set_defaults_on_cfdef(cfdef)
        cfdef.keyspace = self.keyspace.name
        ks.cf_defs.append(cfdef)
        return self.node.schema_code()

    def set_defaults_on_cfdef(self, cfdef):
        if not valid_cf_name_re.match(cfdef.name):
            raise types.InvalidRequestException(
                why='Invalid columnfamily name %r' % cfdef.name)
        if cfdef.comparator_type is None:
            cfdef.comparator_type = 'BytesType'
        if cfdef.subcomparator_type is None:
            cfdef.subcomparator_type = 'BytesType'
        try:
            vtype = self.service.load_class_by_java_name(cfdef.comparator_type)
            vtype.comparator
            cfdef.comparator_type = vtype.java_name
            if cfdef.column_type == 'Super':
                vstype = self.service.load_class_by_java_name(
                    cfdef.subcomparator_type)
                vstype.comparator
                cfdef.subcomparator_type = vstype.java_name
        except (KeyError, AttributeError), e:
            raise types.InvalidRequestException(
                why='Invalid comparator or subcomparator class: %s' % (
                    e.args[0],))
        if cfdef.gc_grace_seconds is None:
            cfdef.gc_grace_seconds = 864000
        if cfdef.column_metadata is None:
            cfdef.column_metadata = []
        if cfdef.default_validation_class is None:
            cfdef.default_validation_class = cfdef.comparator_type
        cfdef.id = id(cfdef) & 0xffffffff
        if cfdef.min_compaction_threshold is None:
            cfdef.min_compaction_threshold = 4
        if cfdef.max_compaction_threshold is None:
            cfdef.max_compaction_threshold = 32
        if cfdef.row_cache_save_period_in_seconds is None:
            cfdef.row_cache_save_period_in_seconds = 0
        if cfdef.key_cache_save_period_in_seconds is None:
            cfdef.key_cache_save_period_in_seconds = 14400

    def system_drop_column_family(self, cfname):
        ks = self.keyspace
        newlist = [cf for cf in ks.cf_defs if cf.name != cfname]
        if len(newlist) == len(ks.cf_defs):
            raise types.InvalidRequestException(why='no such CF %r' % cfname)
        ks.cf_defs = newlist
        try:
            del self.service.data[ks.name][cfname]
        except KeyError:
            pass
        return self.node.schema_code()

    def system_add_keyspace(self, ksdef):
        if ksdef.name in self.service.keyspaces:
            raise types.InvalidRequestException(
                why='keyspace %r already exists' % ksdef.name)
        if not valid_ks_name_re.match(ksdef.name):
            raise types.InvalidRequestException(
                why='invalid keyspace name %r' % ksdef.name)
        try:
            ksdef.strat = self.service.load_class_by_java_name(
                ksdef.strategy_class)
        except KeyError:
            raise types.InvalidRequestException(
                why='Unable to find replication strategy %r' %
                    (ksdef.strategy_class,))
        for cf in ksdef.cf_defs:
            cf.keyspace = ksdef.name
            self.set_defaults_on_cfdef(cf)
        self.service.keyspaces[ksdef.name] = ksdef
        return self.node.schema_code()

    def system_drop_keyspace(self, ksname):
        try:
            del self.service.keyspaces[ksname]
        except KeyError:
            raise types.InvalidRequestException(
                why='no such keyspace %r' % ksname)
        try:
            del self.service.data[ksname]
        except KeyError:
            pass
        return self.node.schema_code()

    def system_update_keyspace(self, ksdef):
        if not valid_ks_name_re.match(ksdef.name):
            raise types.InvalidRequestException(
                why='invalid keyspace name %r' % ksdef.name)
        self.service.get_keyspace(ksdef.name)
        try:
            ksdef.strat = self.service.load_class_by_java_name(
                ksdef.strategy_class)
        except KeyError:
            raise types.InvalidRequestException(
                why='Unable to find replication strategy %r' % (
                    ksdef.strategy_class,))
        for cf in ksdef.cf_defs:
            cf.keyspace = ksdef.name
            self.set_defaults_on_cfdef(cf)
        self.service.keyspaces[ksdef.name] = ksdef
        return self.node.schema_code()

    def system_update_column_family(self, cfdef):
        try:
            oldcf = self.service.lookup_cf(self.keyspace, cfdef.name)
        except KeyError:
            raise types.InvalidRequestException(
                why='no such columnfamily %s' % cfdef.name)
        self.set_defaults_on_cfdef(cfdef)
        # can't change some attributes
        for attr in ('keyspace', 'comparator_type', 'subcomparator_type'):
            if getattr(cfdef, attr, None) != getattr(oldcf, attr, None):
                raise types.InvalidRequestException(
                    why="can't change %s" % attr)
        for attr, val in cfdef.__dict__.iteritems():
            if attr != 'id':
                setattr(oldcf, attr, val)
        return self.node.schema_code()

    def execute_cql_query(query, compression):
        raise types.InvalidRequestException(why='CQL unsupported here')

    @staticmethod
    def filter_by_col_names(cols, colnames):
        results = []
        for n in colnames:
            try:
                results.append(cols[n])
            except KeyError:
                pass
        return results

    def filter_by_slice_range(self, column_parent, cols, slicerange):
        compar = self.service.comparator_for(self.keyspace, column_parent)
        colpred = self.service.slice_predicate(compar, slicerange)
        count = slicerange.count
        if count == 0:
            return []
        name = cols.pop(None)
        items = sorted(cols.iteritems(), cmp=compar, key=lambda i:i[0],
                       reverse=bool(slicerange.reversed))
        cols[None] = name
        filtered = []
        for cname, col in items:
            if colpred(cname):
                filtered.append(col)
                if len(filtered) >= count:
                    break
        return filtered

    def filter_by_predicate(self, column_parent, cols, predicate):
        if predicate.column_names is not None:
            # yes, ignore slice_range even if set)
            return self.filter_by_col_names(cols, predicate.column_names)
        else:
            return self.filter_by_slice_range(
                column_parent, cols, predicate.slice_range)

    def pack_up_supercolumn(self, keyspace, column_parent, dat):
        name = dat.pop(None)
        cp = types.ColumnParent(
            column_family=column_parent.column_family, super_column=name)
        compar = self.service.comparator_for(self.keyspace, cp)
        cols = dat.values()
        cols.sort(cmp=compar, key=lambda c:c.name)
        dat[None] = name
        return types.SuperColumn(name=name, columns=cols)
