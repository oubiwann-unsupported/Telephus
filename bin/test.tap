import os

from twisted.application import service

from telephus.testing.cassanova import CassanovaService


application = service.Application('cassanova')
c = CassanovaService(int(os.environ.get('CASSANOVA_CLUSTER_PORT', 12379)))
c.add_node('127.0.0.1')
c.setServiceParent(application)
