from telephus.testing.cassanova import base


class ReplicationStrategy(base.JavaMimicClass):
    pass


class SimpleStrategy(ReplicationStrategy):
    java_name = 'org.apache.cassandra.locator.SimpleStrategy'


class OldNetworkTopologyStrategy(ReplicationStrategy):
    java_name = 'org.apache.cassandra.locator.OldNetworkTopologyStrategy'


class NetworkTopologyStrategy(ReplicationStrategy):
    java_name = 'org.apache.cassandra.locator.NetworkTopologyStrategy'


class LocalStrategy(ReplicationStrategy):
    java_name = 'org.apache.cassandra.locator.LocalStrategy'
