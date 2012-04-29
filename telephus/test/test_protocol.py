import os

from twisted.internet import defer, error, reactor
from twisted.python.failure import Failure
from twisted.trial import unittest

from telephus import translate
from telephus.cassandra.ttypes import *
from telephus.client import CassandraClient
from telephus.protocol import ManagedCassandraClientFactory, APIMismatch
from telephus.translate import (
    CASSANDRA_08_VERSION, thrift_api_ver_to_cassandra_ver)


CONNS = 5
HOST = os.environ.get('CASSANDRA_HOST', 'localhost')
PORT = 9160
KEYSPACE = 'TelephusTests'
T_KEYSPACE = 'TelephusTests2'
CF = 'Standard1'
SCF = 'Super1'
COUNTER_CF = 'Counter1'
SUPERCOUNTER_CF = 'SuperCounter1'
IDX_CF = 'IdxTestCF'
T_CF = 'TransientCF'
T_SCF = 'TransientSCF'
COLUMN = 'foo'
COLUMN2 = 'foo2'
SCOLUMN = 'bar'
# until Cassandra supports these again..
DO_SYSTEM_RENAMING = False


class ManagedCassandraClientFactoryTest(unittest.TestCase):
    @defer.inlineCallbacks
    def test_initial_connection_failure(self):
        cmanager = ManagedCassandraClientFactory()
        client = CassandraClient(cmanager)
        d = cmanager.deferred
        reactor.connectTCP('nonexistent.example.com', PORT, cmanager)
        yield self.assertFailure(d, error.DNSLookupError)
        cmanager.shutdown()

    @defer.inlineCallbacks
    def test_api_match(self):
        cmanager = ManagedCassandraClientFactory(require_api_version=None)
        client = CassandraClient(cmanager)
        d = cmanager.deferred
        conn = reactor.connectTCP(HOST, PORT, cmanager)
        yield d
        yield client.describe_schema_versions()
        api_ver = cmanager._protos[0].api_version
        yield cmanager.shutdown()

        # try with the right version explicitly required
        cmanager = ManagedCassandraClientFactory(require_api_version=api_ver)
        client = CassandraClient(cmanager)
        d = cmanager.deferred
        conn = reactor.connectTCP(HOST, PORT, cmanager)
        yield d
        yield client.describe_schema_versions()
        yield cmanager.shutdown()

        # try with a mismatching version explicitly required
        bad_ver = [v for (_, v) in translate.supported_versions if v != api_ver][0]
        cmanager = ManagedCassandraClientFactory(require_api_version=bad_ver)
        client = CassandraClient(cmanager)
        d = cmanager.deferred
        conn = reactor.connectTCP(HOST, PORT, cmanager)
        yield self.assertFailure(d, translate.APIMismatch)
        yield cmanager.shutdown()
