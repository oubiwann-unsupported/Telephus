from telephus.testing.cassanova import base


class Partitioner(base.JavaMimicClass):
    min_bound = ''
    key_cmp = staticmethod(cmp)

    @classmethod
    def token_as_bytes(cls, tok):
        return tok


class RandomPartitioner(Partitioner):
    java_name = 'org.apache.cassandra.dht.RandomPartitioner'

    @classmethod
    def token_as_bytes(cls, tok):
        return '%d' % tok
