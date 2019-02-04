The tests run against a MongoDB cluster which is
configured and started externally to the test suite. This allows
running the entire test suite against, for example, a standalone
mongod as well as a replica set. The flip side to this is the
test suite will not work without a running mongo cluster, and
tests which are not applicable to or cannot be performed on the
running mongo cluster are skipped.

Not only does the test suite require an externally launched cluster,
the test suite must also be told how the cluster is configured
via MONGODB_URI, TOPOLOGY, MONGODB_ADDRESSES, RS_ENABLED, RS_NAME and/or
SHARDED_ENABLED environment variables.

The test suite attempts to provide diagnostics when it is not able to
connect to the cluster it is configured to use.

Additionally some of the tests assume that the seed list (given in
MONGODB_URI or MONGODB_ADDRESSES) encompasses all servers in the cluster,
and will fail when MONGODB_URI includes only one host of a replica set.
It is best to include all hosts of the cluster in MONGODB_URI and
MONGODB_ADDRESSES.

It is best to have the test suite configured to connect to exactly
the hostnames configured in the cluster. If, for example, the test suite
is configured to use IP addresses but the cluster is configured with
hostnames, the tests should still work (by using SDAM to discover correct
cluster configuration) but will spend a significant amount of extra time
on server discovery.

In order to run spec tests, the mongo cluster needs to have fail points
enabled. This is accomplished by starting mongod with the following option:
  --setParameter enableTestCommands=1

Use the following environment variables to configure the tests:

CLIENT_DEBUG: Show debug messages from the client.

    CLIENT_DEBUG=1

MONGODB_URI: Connection string to use. This must be a valid MongoDB URI;
mongodb:// and mongodb+srv:// are both supported.
RS_ENABLED and SHARDED_ENABLED are NOT honored if using MONGODB_URI -
specify replica set name in the URI and to specify a sharded topology
set TOPOLOGY=sharded_cluster environment variable.

    MONGODB_URI=mongodb://127.0.0.1:27001/?replicaSet=test
    MONGODB_URI=mongodb://127.0.0.1:27001,127.0.0.1:27002/ TOPOLOGY=sharded_cluster

MONGODB_ADDRESSES: Specify addresses to connect to. Use RS_ENABLED,
RS_NAME and SHARDED_ENABLED to configure the topology.

    MONGODB_ADDRESSES=127.0.0.1:27017,127.0.0.1:27018
    MONGODB_ADDRESSES=127.0.0.1:27017,127.0.0.1:27018 RS_ENABLED=1
    MONGODB_ADDRESSES=127.0.0.1:27017,127.0.0.1:27018 RS_ENABLED=1 RS_NAME=test
    MONGODB_ADDRESSES=127.0.0.1:27017,127.0.0.1:27018 SHARDED_ENABLED=1

RS_ENABLED: Instruct the test suite to connect to a replica set.
RS_ENABLED is only honored when not using MONGODB_URI; to connect to a
replica set with MONGODB_URI, specify the replica set name in the URI
(despite the Ruby driver performing topology discovery by default, it
doesn't do so in the test suite).
RS_NAME can be given to specify the replica set name; the default is
ruby-driver-rs.

    RS_ENABLED=1
    RS_ENABLED=1 RS_NAME=test

SHARDED_ENABLED: Instruct the test suite to connect to the sharded cluster.
Set MONGODB_URI appropriately as well.

    SHARDED_ENABLED=1

SSL_ENABLED: Instruct the test suite to connect to the cluster via SSL.

    SSL_ENABLED=1
    # Also acceptable:
    SSL=ssl

Note: SSL can also be enabled by giving ssl=true in the MONGODB_URI options.

EXTERNAL_DISABLED: Run the tests without making any external connections
(for example, external connections are required to test DNS seedlists and SRV
URIs).

    EXTERNAL_DISABLED=true
