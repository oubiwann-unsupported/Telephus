Telephus
========

Son of Heracles who loved Cassandra. He went a little crazy, at one point. One
might almost say he was twisted.

Description
-----------

Telephus is a connection pooled, low-level client API for Cassandra in Twisted
(Python).


Installation
------------

Prerequisites:

* Python >= 2.4 (2.5 or later for tests)
* Twisted 8.1.0 or later
* Thrift (latest svn)


Usage
-----

See example.py for an example of how to use the Telephus API.


Unit Tests
----------

To run the unit tests, simply use the Twisted test runner, ``trial``::

 $ trial telephus

The tests are included in the source code, so this should work even if you are
not running it from the source checkout directory.

Note that the unit tests are not pure unit tests, but rather functional and/or
integration tests: they depend upon a running instance of Cassandra (they don't
have network/db connections mocked yet). As such, you will see a great many
errors if you attempt to run the tests without first having installed and
started Cassandra.

If you wish to run the cluster pool tests, you will need to have the Python
Cassanova library installed. You can download it from here:

 * https://github.com/riptano/Cassanova
