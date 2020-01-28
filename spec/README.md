# Running Ruby Driver Tests

## Quick Start

To run the test suite against a local MongoDB deployment listening on port
27017, run:

    rake

When run without options, the test suite will automatically detect deployment
topology and configure itself appropriately. Standalone, replica set and
sharded cluster topologies are supported (though the test suite will presently
use only the first listed shard in a sharded cluster if given a seed list,
or the one running on port 27017 if not given a seed list).

TLS, authentication and other options can be configured via URI options by
setting `MONGODB_URI` environment variable appropriately. Examples of such
configuration are given later in this document.

## MongoDB Server Deployment

The tests require a running MongoDB deployment, configured and started
externally to the test suite.

Tests that are not appropriate for the running deployment will be skipped,
with one exception: the test suite assumes that fail points are enabled in
the deployment (see the Fail Points section below). Not every test uses fail
points, therefore it is possible to launch the server without fail points
being enabled and still pass many of the tests in the test suite.

## Starting MongoDB Deployment

There are many ways in which MongoDB can be started. The instructions below
are for manually launching `mongod` instances and using
[mlaunch](http://blog.rueckstiess.com/mtools/mlaunch.html)
(part of [mtools](https://github.com/rueckstiess/mtools)) for more complex
deployments, but other tools like
[mongodb-runner](https://github.com/mongodb-js/runner) and
[Mongo Orchestration](https://github.com/10gen/mongo-orchestration) can
in principle also work.

### Standalone

The simplest possible deployment is a standalone `mongod`, which can be
launched as follows:

    # Launch mongod in one terminal
    mkdir /tmp/mdb
    mongod --dbpath /tmp/mdb --setParameter enableTestCommands=1

    # Run tests in another terminal
    rake

A standalone deployment is a good starting point, however a great many tests
require a replica set deployment and will be skipped on a standalone deployment.

### Replica Set

While a replica set can be started and configured by hand, doing so is
cumbersome. The examples below use
[mlaunch](http://blog.rueckstiess.com/mtools/mlaunch.html)
to start a replica set.

First, install [mtools](https://github.com/rueckstiess/mtools):

    pip install 'mtools[mlaunch]' --user -U --upgrade-strategy eager
    # On Linux:
    export PATH=~/.local/bin:$PATH
    # On MacOS:
    export PATH=$PATH:~/Library/Python/2.7/bin

Then, launch a replica set:

    mlaunch init --replicaset --name ruby-driver-rs \
      --dir /tmp/mdb-rs --setParameter enableTestCommands=1

The test suite willl automatically detect the topology, no explicit
configuration is needed:

    rake

### Replica Set With Arbiter

Some tests require an arbiter to be present in the replica set. Such a
deployment can be obtained by providing `--arbiter` argument to mlaunch:

    mlaunch init --replicaset --arbiter --name ruby-driver-rs \
      --dir /tmp/mdb-rs --setParameter enableTestCommands=1

To indicate to the test suite that the deployment contains an arbiter, set
HAVE_ARBITER environment variable as follows:

    HAVE_ARBITER=1 rake

### Sharded Cluster

A sharded cluster can be configured with mlaunch:

    mlaunch init --replicaset --name ruby-driver-rs --sharded 1 --mongos 2 \
      --dir /tmp/mdb-sc --setParameter enableTestCommands=1

As with the replica set, the test suite will automatically detect sharded
cluster topology.

Note that some tests require a sharded cluster with exactly one shard and
other tests require a sharded cluster with more than one shard. Tests requiring
a single shard can be run against a deployment with multiple shards by
specifying only one mongos address in MONGODB_URI.

## Note Regarding SSL/TLS Arguments

MongoDB 4.2 (server and shell) added new command line options for setting TLS
parameters. These options follow the naming of URI options used by both the
shell and MongoDB drivers starting with MongoDB 4.2. The new options start with
the `--tls` prefix.

Old options, starting with the `--ssl` prefix, are still supported for backwards
compatibility, but their use is deprecated. As of this writing, mlaunch only
supports the old `--ssl` prefix options.

In the rest of this document, when TLS options are given for `mongo` or
`mongod` they use the new `--tls` prefixed arguments, and when the same options
are given to `mlaunch` they use the old `--ssl` prefixed forms. The conversion
table of the options used herein is as follows:

| --tls prefixed option   | --ssl prefixed option |
| ----------------------- | --------------------- |
| --tls                   | --ssl                 |
| --tlsCAFile             | --sslCAFile           |
| --tlsCertificateKeyFile | --sslPEMKeyFile       |

## TLS With Verification

The test suite includes a set of TLS certificates for configuring a server
and a client to perform full TLS verification in the `spec/support/certificates`
directory. The server can be started as follows, if the current directory is
the top of the driver source tree:

    mlaunch init --single --dir /tmp/mdb-ssl --sslMode requireSSL \
      --sslPEMKeyFile `pwd`/spec/support/certificates/server.pem \
      --sslCAFile `pwd`/spec/support/certificates/ca.pem \
      --sslClientCertificate `pwd`/spec/support/certificates/client.pem

To test that the driver works when the server's certificate is signed by an
intermediate certificate (i.e. uses certificate chaining), use the chained
server certificate bundle:

    mlaunch init --single --dir /tmp/mdb-ssl --sslMode requireSSL \
      --sslPEMKeyFile `pwd`/spec/support/certificates/server-second-level-bundle.pem \
      --sslCAFile `pwd`/spec/support/certificates/ca.pem \
      --sslClientCertificate `pwd`/spec/support/certificates/client.pem

The driver's test suite is configured to verify certificates by default.
If the server is launched with the certificates from the driver's test suite,
the test suite can be run simply by specifying `tls=true` URI option:

    MONGODB_URI='mongodb://localhost:27017/?tls=true' rake

The driver's test suite can also be executed against a server launched with
any other certificates. In this case the certificates need to be explicitly
specified in the URI, for example as follows:

    MONGODB_URI='mongodb://localhost:27017/?tls=true&tlsCAFile=path/to/ca.crt&tlsCertificateKeyFile=path/to/client.pem' rake

Note that some tests (specifically testing TLS verification) expect the server
to be launched using the certificates in the driver's test suite, and will
fail when run against a server using other certificates.

## TLS Without Verification

It is also possible to enable TLS but omit certificate verification. In this
case a standalone server can be started as follows:

    mlaunch init --single --dir /tmp/mdb-ssl --sslMode requireSSL \
      --sslPEMKeyFile `pwd`/spec/support/certificates/server.pem \
      --sslCAFile `pwd`/spec/support/certificates/ca.pem \
      --sslAllowConnectionsWithoutCertificates \
      --sslAllowInvalidCertificates

To run the test suite against such a server, also omitting certificate
verification, run:

    MONGODB_URI='mongodb://localhost:27017/?tls=true&tlsInsecure=true' rake

Note that there are tests in the test suite that cover TLS verification, and
they may fail if the test suite is run in this way.

## Authentication

mlaunch can configure authentication on the server:

    mlaunch init --single --dir /tmp/mdb-auth --auth --username dev --password dev

To run the test suite against such a server, run:

    MONGODB_URI='mongodb://dev:dev@localhost:27017/' rake

## X.509 Authentication

Note: Testing X.509 authentication requires an enterprise build of the MongoDB
server.

To set up a server configured for authentication with an X.509 certificate,
first launch a TLS-enabled server with a regular credentialed user.

The credentialed user is required because mlaunch configures `--keyFile`
option for cluster member authentication, which in turn enables authentication.
With authentication enabled, `mongod` allows creating the first user in the
`admin` database but the X.509 user must be created in the `$external`
database - as a result, the X.509 user cannot be the only user in the deployment.

Run the following command to set up a standalone `mongod` with a bootstrap
user:

    mlaunch init --single --dir /tmp/mdb-x509 --sslMode requireSSL \
      --sslPEMKeyFile `pwd`/spec/support/certificates/server.pem \
      --sslCAFile `pwd`/spec/support/certificates/ca.crt \
      --sslClientCertificate `pwd`/spec/support/certificates/client.pem \
      --auth --username bootstrap --password bootstrap

Next, create the X.509 user. The command to create the user is the same
across all supported MongoDB versions, and for convenience we assign its text
to a variable as follows:

    create_user_cmd="`cat <<'EOT'
      db.getSiblingDB("$external").runCommand(
        {
          createUser: "C=US,ST=New York,L=New York City,O=MongoDB,OU=x509,CN=localhost",
          roles: [
               { role: "dbAdminAnyDatabase", db: "admin" },
               { role: "readWriteAnyDatabase", db: "admin" },
               { role: "userAdminAnyDatabase", db: "admin" },
               { role: "clusterAdmin", db: "admin" },
          ],
          writeConcern: { w: "majority" , wtimeout: 5000 },
        }
      )
    EOT
    `"

Use the MongoDB shell to execute this command:

    mongo --tls \
      --tlsCAFile `pwd`/spec/support/certificates/ca.crt \
      --tlsCertificateKeyFile `pwd`/spec/support/certificates/client-x509.pem \
      -u bootstrap -p bootstrap \
      --eval "$create_user_cmd"

Verify that authentication is required by running the following command, which
should fail:

    mongo --tls \
      --tlsCAFile `pwd`/spec/support/certificates/ca.crt \
      --tlsCertificateKeyFile `pwd`/spec/support/certificates/client-x509.pem \
      --eval 'db.serverStatus()'

Verify that X.509 authentication works by running the following command:

    mongo --tls \
      --tlsCAFile `pwd`/spec/support/certificates/ca.crt \
      --tlsCertificateKeyFile `pwd`/spec/support/certificates/client-x509.pem \
      --authenticationDatabase '$external' \
      --authenticationMechanism MONGODB-X509 \
      --eval 'db.serverStatus()'

The test suite includes a set of integration tests for X.509 client authentication.

To run the test suite against such a server, run:

    MONGODB_URI="mongodb://localhost:27017/?authMechanism=MONGODB-X509&tls=true&tlsCAFile=spec/support/certificates/ca.crt&tlsCertificateKeyFile=spec/support/certificates/client-x509.pem" rake

## Field-Level Encryption

Install libmongocrypt on your machine:

Option 1: Download a pre-built binary
- Download a tarball of all libmongocrypt variations from this link:
    https://s3.amazonaws.com/mciuploads/libmongocrypt/all/master/latest/libmongocrypt-all.tar.gz
- Unzip the file you downloaded. You will see a list of folders, each
    corresponding to an operating system. Find the folder that matches
    your operating system and open it.
- Inside that folder, open the folder called "nocrypto." In either the
    lib or lb64 folder, you will find the libmongocrypt.so or
    libmongocrypt.dylib or libmongocrypt.dll file, depending on your OS.
- Move that file to wherever you want to keep it on your machine.

Option 2: Build from source
- To build libmongocrypt from source, follow the instructions in the README on the libmongocrypt GitHub repo: https://github.com/mongodb/libmongocrypt

Create AWS KMS keys
Many of the Field-Level Encryption tests require that you have an encryption
master key hosted on AWS's Key Management Service. Set up a master key by following
these steps:

1. Sign up for an AWS account at this link if you don't already have one: https://aws.amazon.com/resources/create-account/

2. Create a new IAM user that you want to have permissions to access your new
master key by following this guide: the "Creating an Administrator IAM User and Group (Console)"
section of this guide: https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user.html

3. Create an access key for your new IAM user and store the access key credentials
in environment variables on your local machine. Create an access key by following the
"Managing Access Keys (Console)" instructions in this guide:
https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html#Using_CreateAccessKey
Once an access key has been created, store the access key id and the access key
secret in environment variables. If you plan to frequently run Field-Level
Encryption tests, it may be a good idea to put these lines in your .bash_profile
or .bashrc file. Otherwise, you can run them in the terminal window where you
plan to run your tests.

```
export MONGO_RUBY_DRIVER_AWS_KEY="YOUR-ACCESS-KEY-ID"
export MONGO_RUBY_DRIVER_AWS_SECRET="YOUR-ACCESS-KEY-SECRET"
```

4. Create a new symmetric Customer Master Key (CMK) by following the "Creating Symmetric CMKs (Console)"
section of this guide: https://docs.aws.amazon.com/kms/latest/developerguide/create-keys.html

5. Give your IAM user "Key administrator" and "Key user" privileges on your new CMK
by following the "Using the AWS Management Console Default View" section of this guide:
https://docs.aws.amazon.com/kms/latest/developerguide/key-policy-modifying.html

TODO: explain where in the test suite to store CMK information (not yet relevant)

In one terminal, launch MongoDB:

NOTE: You must be running MongoDB 4.2 or higher. All auto-encryption features
require an enterprise build of MongoDB, but you can still run
explicit encryption tests using the community edition of MongoDB.

Download different versions of MongoDB here: https://www.mongodb.com/download-center/enterprise

```
mkdir /tmp/mdb
mongod --dbpath /tmp/mdb --setParameter enableTestCommands=1
```

In another terminal run the tests, making sure to set the `LIBMONGOCRYPT_PATH`
environment variable to the full path to the .so/.dll/.dylib
```
LIBMONGOCRYPT_PATH=/path/to/your/libmongocrypt/nocrypto/libmongocrypt.so bundle exec rake
```

## Compression

To be written.

## Other Options

Generally, all URI options recognized by the driver may be set for a test run,
and will cause the clients created by the test suite to have those options
by default. For example, retryable writes may be turned on and off as follows:

    MONGODB_URI='mongodb://localhost:27017/?retryWrites=true' rake

    MONGODB_URI='mongodb://localhost:27017/?retryWrites=false' rake

Individual tests may override options that the test suite uses as defaults.
For example, retryable writes tests may create clients with the retry writes
option set to true or false as needed regardless of what the default is for
the entire test run.

It is also possible to, for example, reference non-default hosts and replica
set names:

    MONGODB_URI='mongodb://test.host:27017,test.host:27018/?replicaSet=fooset' rake

However, as noted in the caveats section, changing the database name used by
the test suite is not supported.

Some tests require internet connectivity, for example to test DNS seed lists
and SRV URIs. These tests can be skipped by setting the following environment
variable:

    EXTERNAL_DISABLED=true

## Caveats

### Socket Permission Errors

If you get permission errors connecting to `mongod`'s socket, adjust its
permissions:

    sudo chmod 0666 /tmp/mongodb-27017.sock

Alternatively, specify the following argument to `mlaunch` or `mongod`:

    --filePermissions 0666

### Non-Identical Hostnames

The test suite should be configured to connect to exactly the hostnames
configured in the cluster. If, for example, the test suite is configured
to use IP addresses but the cluster is configured with hostnames, most tests
would still work (by using SDAM to discover correct cluster configuration)
but will spend a significant amount of extra time on server discovery.

Some tests perform address assertions and will fail if hostnames configured
in the test suite do not match hostnames configured in the cluster.
For the same reason, each node in server configuration should have its port
specified.

### Database Name

The test suite currently does not allow changing the database name that it
uses, which is `ruby-driver`. Attempts to specify a different database name
in the URI for example will lead to some of the tests failing.

### Fail Points

In order to run some of the tests, the mongo cluster needs to have fail points
enabled. This is accomplished by starting `mongod` with the following option:

    --setParameter enableTestCommands=1

### Log Output

The test suite is run with the driver log level set to WARN by default.
This produces a fair amount of output as many tests trigger various conditions
resulting in the driver outputting warnings. This is expected behavior.

## Running Individual Examples

Individual examples can be run by invoking `rspec` instead of `rake`. Prior
to running `rspec`, ensure the test suite created users for itself - this
is done by the `rake` command automatically, or you can manually invoke the
Rake task which configures the deployment for testing:

    rake spec:prepare

Then, any of the standard RSpec invocations will work:

    rspec path/to/file_spec.rb

## Configuration Reporting

To have the test suite report its current configuration, run:

    rake spec:config
