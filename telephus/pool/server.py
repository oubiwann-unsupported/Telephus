import random
from time import time

from twisted.internet import protocol


class CassandraNode(object):
    """
    Represent a Cassandra node, in the same sense Cassandra uses.

    Keep track of connection success and failure history for some time, so
    that smarter decisions can be made about where to make new connections
    within a pool.

    Implement exponential backoff in reconnect time when connections fail.

    @ivar history_interval: Keep history entries for at least this many seconds

    @ivar max_delay: Forced delay between connection attempts will not exceed
        this value (although actual connection attempts may be farther apart
        than this, if the pool has enough connections without it)
    """

    history_interval = 86400
    max_delay = 180
    initial_delay = 0.05

    # NIST backoff factors
    factor = protocol.ReconnectingClientFactory.factor
    jitter = protocol.ReconnectingClientFactory.jitter

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.reconnect_delay = self.initial_delay
        self.can_reconnect_at = 0

        # a list of (timestamp, error) tuples, least recent first.
        # (timestamp, None) tuples will be inserted on a successful connection.
        self.history = []

    def record_hist(self, value):
        now = time()
        if (self.history and
            self.history[0][0] < (now - self.history_interval * 2)):
            # it has been 2x history_interval; prune history
            cutoff = now - self.history_interval
            for n, (tstamp, hval) in enumerate(self.history):
                if tstamp > cutoff:
                    break
            self.history = self.history[n:]
        self.history.append((now, value))

    def conn_success(self):
        self.reconnect_delay = self.initial_delay
        self.can_reconnect_at = 0
        self.record_hist(None)

    def conn_fail(self, reason):
        # these tend to come in clusters. if the same error was received
        # recently (before the reconnect delay expired), return False to
        # indicate the event is not 'notable', and don't bump the delay
        # to a higher level.
        is_notable = self.is_failure_notable(reason)
        self.record_hist(reason.value)
        if is_notable:
            newdelay = min(self.reconnect_delay * self.factor, self.max_delay)
            if self.jitter:
                newdelay = random.normalvariate(
                    newdelay, newdelay * self.jitter)
            self.reconnect_delay = newdelay
            self.can_reconnect_at = time() + newdelay
        else:
            # reset but use the same delay
            self.can_reconnect_at = time() + self.reconnect_delay
        return is_notable

    def is_failure_notable(self, reason):
        try:
            tstamp, last_err = self.history[-1]
        except IndexError:
            pass
        else:
            if type(last_err) is type(reason.value):
                if time() < self.can_reconnect_at:
                    return False
        return True

    def seconds_until_connect_ok(self):
        return self.can_reconnect_at - time()

    def __str__(self):
        return '<%s %s:%s @0x%x>' % (self.__class__.__name__,
                                     self.host, self.port, id(self))

    __repr__ = __str__

    def __eq__(self, other):
        return self.__class__ == other.__class__ \
           and self.host == other.host \
           and self.port == other.port

    def __cmp__(self, other):
        return cmp((self.host, self.port), (other.host, other.port))

    def __hash__(self):
        return hash((self.__class__, self.host, self.port))
