import struct

from telephus.testing.cassanova import base


class AbstractType(base.JavaMimicClass):
    @classmethod
    def input(cls, mybytes):
        return mybytes

    @classmethod
    def comparator(cls, a, b):
        return cmp(cls.input(a), cls.input(b))


class BytesType(AbstractType):
    java_name = 'org.apache.cassandra.db.marshal.BytesType'


class LongType(AbstractType):
    java_name = 'org.apache.cassandra.db.marshal.LongType'

    @classmethod
    def input(cls, mybytes):
        if len(mybytes) < 8:
            mybytes = '\x00' * (8 - len(mybytes)) + mybytes
        elif len(mybytes) > 8:
            raise ValueError('Bad input %r to LongType' % (mybytes,))
        return struct.unpack('!q', mybytes)[0]


class IntegerType(AbstractType):
    java_name = 'org.apache.cassandra.db.marshal.IntegerType'


class UTF8Type(AbstractType):
    java_name = 'org.apache.cassandra.db.marshal.UTF8Type'


class TimeUUIDType(AbstractType):
    java_name = 'org.apache.cassandra.db.marshal.TimeUUIDType'
