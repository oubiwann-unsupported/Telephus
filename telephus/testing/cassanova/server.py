from cStringIO import StringIO
import hashlib
import random

from twisted.application import internet
from twisted.python import log

from thrift.protocol import TBinaryProtocol
from thrift.transport import TTwisted, TTransport

from telephus.cassandra.c08 import Cassandra
from telephus.testing.cassanova import api


class CassanovaServerProtocol(TTwisted.ThriftServerProtocol):
    """
    """
    def processError(self, error):
        # stoopid thrift doesn't log this stuff
        log.err(error, 'Error causing connection reset')
        TTwisted.ThriftServerProtocol.processError(self, error)

    def stringReceived(self, frame):
        tmi = TTransport.TMemoryBuffer(frame)
        tmo = TTransport.TMemoryBuffer()

        iprot = self.factory.iprot_factory.getProtocol(tmi)
        oprot = self.factory.oprot_factory.getProtocol(tmo)

        self.working = True
        d = self.processor.process(iprot, oprot)
        d.addBoth(self.done_working)
        d.addCallbacks(self.processOk, self.processError,
                       callbackArgs=(tmo,))

    def done_working(self, x):
        self.working = False
        return x

    def connectionMade(self):
        self.working = False
        self.factory.node.conns_to_me.add(self)
        return TTwisted.ThriftServerProtocol.connectionMade(self)

    def connectionLost(self, reason):
        del self.processor
        self.factory.node.conns_to_me.discard(self)
        return TTwisted.ThriftServerProtocol.connectionLost(self, reason)

    def __str__(self):
        return '<%s at 0x%x (working=%s)>' % (
            self.__class__.__name__, id(self), self.working)

    __repr__ = __str__


class CassanovaFactory(TTwisted.ThriftServerFactory):
    """
    """
    protocol = CassanovaServerProtocol
    processor_factory = Cassandra.Processor
    handler_factory = api.Cassanova

    def __init__(self, node):
        self.node = node
        protocolfactory = TBinaryProtocol.TBinaryProtocolFactory()
        TTwisted.ThriftServerFactory.__init__(self, None, protocolfactory)

    def buildProtocol(self, addr):
        """
        This lets us get around Thrift's vehement hatred of statefulness
        in the interface handler; we abuse the protocol instance's "factory"
        attribute to point at a per-connection object.
        """
        p = self.protocol()
        p.factory = self
        p.processor = self.processor_factory(self.handler_factory(self.node))
        return p


class CassanovaNode(internet.TCPServer):
    """
    """
    factory = CassanovaFactory

    def __init__(self, port, interface, token=None):
        internet.TCPServer.__init__(self, port, self.factory(self),
                                    interface=interface)
        self.conns_to_me = set()
        if token is None:
            token = random.getrandbits(127)
        self.mytoken = token

    def startService(self):
        internet.TCPServer.startService(self)
        self.addr = self._port.getHost()

    def stopService(self):
        internet.TCPServer.stopService(self)
        for p in self.conns_to_me:
            p.transport.loseConnection()

    def endpoint_str(self):
        return self.addr.host

    def get_schema(self):
        # in case tests want to override this functionality
        return self.parent.get_schema()

    def schema_code(self):
        return self.make_code(self.serialize_schema_def())

    @staticmethod
    def make_code(info):
        x = hashlib.md5()
        x.update(info)
        digits = x.hexdigest()
        return '-'.join([digits[n:(n+s)]
                         for (n, s) in [(0,8),(8,4),(12,4),(16,4),(20,12)]])

    def serialize_schema_def(self):
        klist = sorted(self.get_schema(), key=lambda k: k.name)
        buf = StringIO()
        for k in klist:
            k.write(TBinaryProtocol.TBinaryProtocol(buf))
        return buf.getvalue()

    def __hash__(self):
        return hash((self.__class__, self.addr, self.mytoken))

    def __str__(self):
        return '<%s at %s:%d>' % (
            self.__class__.__name__, self.addr.host, self.addr.port)

    __repr__ = __str__
