from twisted.python import log


def lame_log_insufficient_nodes(poolsize, pooltarget, pending_reqs, waittime):
    msg = ('(No candidate nodes to expand pool to target size %d from %d; '
           'there are %d pending requests.' % (
            pooltarget, poolsize, pending_reqs))
    if waittime is None:
        msg += ')'
    else:
        msg += ' Expected candidate node retry in %.1f seconds.)' % waittime
    log.msg(msg)


class CassandraKeyspaceConnection:
    """
    Glue class which acts as a manager for CassandraClient but passes requests
    on to a CassandraClusterPool- in the case where you want all requests
    through this manager to be guaranteed to go to the same keyspace,
    regardless of what other consumers of the CassandraClusterPool might do.
    """

    def __init__(self, pool, keyspace):
        self.pool = pool
        self.keyspace = keyspace

    def pushRequest(self, req, retries=None):
        return self.pool.pushRequest(
            req, retries=retries, keyspace=self.keyspace)

    def set_keyspace(self, keyspace):
        raise RuntimeError("Don't call set_keyspace on a "
                           "CassandraKeyspaceConnection")

    def login(self, credentials):
        return self.pool.login(credentials)
