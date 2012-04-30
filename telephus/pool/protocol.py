from warnings import warn

from twisted.internet import defer, protocol, error
from twisted.python import log

from thrift.transport import TTwisted, TTransport
from thrift.protocol import TBinaryProtocol

from telephus import exceptions
from telephus.cassandra.c08 import Cassandra, ttypes
from telephus.protocol import (
    ManagedThriftRequest, ClientBusy, InvalidThriftRequest)
from telephus.translate import (
    APIMismatch, postProcess, thrift_api_ver_to_cassandra_ver, translateArgs)


class CassandraPoolParticipantClient(TTwisted.ThriftClientProtocol):
    thriftFactory = TBinaryProtocol.TBinaryProtocolAcceleratedFactory

    def __init__(self):
        TTwisted.ThriftClientProtocol.__init__(self, Cassandra.Client,
                                               self.thriftFactory())

    def connectionMade(self):
        TTwisted.ThriftClientProtocol.connectionMade(self)
        self.factory.clientConnectionMade(self)

    def connectionLost(self, reason):
        # the TTwisted version of this call does not account for the
        # possibility of other things happening during the errback.
        tex = TTransport.TTransportException(
            type=TTransport.TTransportException.END_OF_FILE,
            message='Connection closed (%s)' % reason)
        while self.client._reqs:
            k = iter(self.client._reqs).next()
            v = self.client._reqs.pop(k)
            v.errback(tex)
        del self.client._reqs
        del self.client


class CassandraPoolReconnectorFactory(protocol.ClientFactory):
    protocol = CassandraPoolParticipantClient
    connector = None
    last_error = None
    noisy = False

    # store the keyspace this connection is set to. we will take thrift
    # requests along with the keyspace in which they expect to be made, and
    # change keyspaces if necessary. this is done this way to avoid having
    # another layer of queueing for requests in this class (in addition to the
    # queue in CassandraClusterPool), or special logic here to pass on
    # set_keyspace calls from the service at the right time (so already-queued
    # requests still get made in their right keyspaces).
    keyspace = None

    def __init__(self, node, service, api_version=None):
        self.node = node
        # if self.service is None, don't bother doing anything. nobody loves
        # us.
        self.service = service
        self.my_proto = None
        self.job_d = self.jobphase = None
        self.api_version = api_version

    def clientConnectionMade(self, proto):
        assert self.my_proto is None
        assert self.jobphase is None, 'jobphase=%r' % (self.jobphase,)
        if self.service is None:
            proto.transport.loseConnection()
        else:
            self.my_proto = proto
            self.service.client_conn_made(self)

    def clientConnectionFailed(self, connector, reason):
        assert self.my_proto is None
        assert self.jobphase is None, 'jobphase=%r' % (self.jobphase,)
        self.my_proto = None
        if self.service is not None:
            self.connector = connector
            self.service.client_conn_failed(reason, self)

    def clientConnectionLost(self, connector, reason):
        self.logstate('clientConnectionLost')
        p = self.my_proto
        self.my_proto = None
        self.stop_working_on_queue()
        if p is not None and self.service is not None:
            self.connector = connector
            self.service.client_conn_lost(self, reason)

    def stopFactory(self):
        # idempotent
        self.logstate('stopFactory')
        protocol.ClientFactory.stopFactory(self)
        if self.connector:
            try:
                self.connector.stopConnecting()
            except error.NotConnectingError:
                pass
        self.connector = None
        p = self.my_proto
        self.my_proto = None
        self.stop_working_on_queue()
        if p is not None and p.transport is not None:
            p.transport.loseConnection()

    def isConnecting(self):
        if self.connector is None:
            if self.my_proto is None:
                # initial connection attempt
                return True
            else:
                # initial connection succeeded and hasn't died
                return False
        return self.connector.state == 'connecting'

    def retry(self):
        """
        Retry this factory's connection. It is assumed that a previous
        connection was attempted and failed- either before or after a
        successful connection.
        """

        if self.connector is None:
            raise ValueError("No connector to retry")
        if self.service is None:
            return
        self.connector.connect()

    def prep_connection(self, creds=None, keyspace=None):
        """
        Do login and set_keyspace tasks as necessary, and also check this
        node's idea of the Cassandra ring. Expects that our connection is
        alive.

        Return a Deferred that will fire with the ring information, or be
        errbacked if something goes wrong.
        """

        d = self.my_describe_version()

        def check_version(thrift_ver):
            cassver = thrift_api_ver_to_cassandra_ver(thrift_ver)
            if self.api_version is None:
                self.api_version = cassver
            elif self.api_version != cassver:
                raise APIMismatch(
                    "%s is exposing thrift protocol version %s -> "
                    "Cassandra version %s, but %s was expected" % (
                        self.node, thrift_ver, cassver, self.api_version))
        d.addCallback(check_version)
        if creds is not None:
            d.addCallback(lambda _: self.my_login(creds))
        if keyspace is not None:
            d.addCallback(lambda _: self.my_set_keyspace(keyspace))
        d.addCallback(lambda _: self.my_describe_ring(keyspace))
        return d

    # The following my_* methods are for internal use, to facilitate the
    # management of the pool and the queries we get. The user should make
    # use of the methods on CassandraClient.

    def my_login(self, creds):
        return self.execute(
            ManagedThriftRequest(
                'login', ttypes.AuthenticationRequest(credentials=creds))
        )

    def my_set_keyspace(self, keyspace):
        return self.execute(ManagedThriftRequest('set_keyspace', keyspace))

    def my_describe_ring(self, keyspace=None):
        if keyspace is None or keyspace == 'system':
            d = self.my_pick_non_system_keyspace()
        else:
            d = defer.succeed(keyspace)
        d.addCallback(lambda k: self.execute(ManagedThriftRequest(
            'describe_ring', k)))

        def suppress_no_keyspaces_error(f):
            f.trap(exceptions.NoKeyspacesAvailable)
            return ()
        d.addErrback(suppress_no_keyspaces_error)
        return d

    def my_describe_version(self):
        return self.execute(ManagedThriftRequest('describe_version'))

    def my_describe_keyspaces(self):
        return self.execute(ManagedThriftRequest('describe_keyspaces'))

    def my_pick_non_system_keyspace(self):
        """
        Find a keyspace in the cluster which is not 'system', for the purpose
        of getting a valid ring view. Can't use 'system' or null.
        """
        d = self.my_describe_keyspaces()

        def pick_non_system(klist):
            for k in klist:
                if k.name != 'system':
                    return k.name
            err = exceptions.NoKeyspacesAvailable(
                "Can't gather information about the Cassandra ring; no "
                "non-system keyspaces available")
            warn(err)
            raise err
        d.addCallback(pick_non_system)
        return d

    def store_successful_keyspace_set(self, val, ksname):
        self.keyspace = ksname
        return val

    def execute(self, req, keyspace=None):
        if self.my_proto is None:
            return defer.errback(error.ConnectionClosed(
                'Lost connection before %s request could be made' % (
                    req.method,)))
        method = getattr(self.my_proto.client, req.method, None)
        if method is None:
            raise InvalidThriftRequest(
                "don't understand %s request" % req.method)

        d = defer.succeed(0)

        if req.method == 'set_keyspace':
            newksname = req.args[0]
            d.addCallback(lambda _: method(newksname))
            d.addCallback(self.store_successful_keyspace_set, newksname)
        else:
            if keyspace is not None and keyspace != self.keyspace:
                d.addCallback(lambda _: self.my_set_keyspace(keyspace))
            args = translateArgs(req, self.api_version)
            d.addCallback(lambda _: method(*args))
            d.addCallback(lambda results: postProcess(results, req.method))
        return d

    def clear_job(self, x):
        self.logstate('clear job %s (result %r)' % (self.jobphase, x))
        self.jobphase = None
        self.job_d = None
        return x

    def job(self, _name, _func, *args, **kw):
        self.logstate('start job %s' % _name)
        if self.jobphase is not None:
            raise ClientBusy('Tried to start job phase %s while in %s'
                             % (_name, self.jobphase))
        self.jobphase = _name
        d = defer.maybeDeferred(_func, *args, **kw)
        self.job_d = d
        d.addBoth(self.clear_job)
        return d

    def process_request_error(self, err, req, keyspace, req_d, retries):
        self.logstate('process_request_error: %s, retries=%d' % (err, retries))
        self.last_error = err
        if (retries > 0 and self.service is not None and
            err.check(*self.service.retryables)):
            self.logstate('-- resubmit --')
            assert self.jobphase is None, \
                    'Factory might retry its own fatal error'
            self.service.resubmit(req, keyspace, req_d, retries - 1)
        else:
            self.logstate('-- giving up [retries=%d service=%s err=%s] --'
                          % (retries, self.service, err.value))
            req_d.errback(err)

    def work_on_request(self, reqtuple):
        req, keyspace, req_d, retries = reqtuple
        if req_d.called:
            # cancelled while pending in the queue
            return
        d = self.job('pending_request', self.execute, req, keyspace)
        d.addCallback(req_d.callback)
        d.addErrback(self.process_request_error, req, keyspace, req_d, retries)
        return d

    def maybe_do_more_work(self, _, q):
        # it's possible this is being called as part of the immediate callback
        # chain after the protocol's connectionLost errbacking. if so, our own
        # connectionLost hasn't been called yet. allow all current processing
        # to finish before deciding whether we get to do more.
        def _real_maybe_do_more_work():
            if not self.keep_working:
                self.stopFactory()
            elif self.service is not None:
                self.work_on_queue(q)
        if self.service is not None:
            self.service.reactor.callLater(0, _real_maybe_do_more_work)

    def scream_like_a_little_girl(self, fail):
        if self.service is not None:
            complain = self.service.err
        else:
            complain = log.err
        complain(fail, "Factory for connection to %s had problems dealing with"
                       " the queue" % (self.node,))
        # don't process more requests

    def work_on_queue(self, q):
        self.logstate('work_on_queue')
        self.keep_working = True
        d = self.job('queue_getter', q.get)
        d.addCallback(self.work_on_request)
        d.addCallback(self.maybe_do_more_work, q)
        d.addErrback(lambda f: f.trap(defer.CancelledError))
        d.addErrback(self.scream_like_a_little_girl)
        return d

    def stop_working_on_queue(self):
        self.logstate('stop_working_on_queue [jobphase=%s]' % self.jobphase)
        self.keep_working = False
        if self.jobphase == 'queue_getter':
            self.job_d.cancel()

    def finish_and_die(self):
        """
        If there is a request pending, let it finish and be handled, then
        disconnect and die. If not, cancel any pending queue requests and
        just die.
        """
        self.logstate('finish_and_die')
        self.stop_working_on_queue()
        if self.jobphase != 'pending_request':
            self.stopFactory()

    def logstate(self, msg):
        if getattr(self, 'debugging', False):
            log.msg('CPRF 0x%x (node %s) [%s]: %s'
                    % (id(self), self.node, self.jobphase, msg))
