class NoKeyspacesAvailable(UserWarning):
    """
    Indicates CassandraClusterPool could not collect information about the
    cluster ring, in order to automatically add nodes to the pool.

    When Cassandra's thrift interface allows specifying null for describe_ring
    (like the underlying java interface already does), we can remove this.
    """


class NoNodesAvailable(Exception):
    """
    Indicates there are nodes to which we are allowed to make another immediate
    connection. The argument to this exception should be the expected number
    of seconds before a node /will/ be available.

    This should be handled internally; user code is not expected to see or
    handle this type of exception.
    """
