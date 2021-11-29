# Evergreen Tests

This directory contains configuration and scripts used to run the driver's
test suite in Evergreen, MongoDB's continuous integration system.


## Testing In Docker

It is possible to run the test suite in Docker. This executes all of the
shell scripts as if they were running in the Evergreen environment.

Use the following command:

    ./.evergreen/test-on-docker -d debian92 RVM_RUBY=ruby-2.7

The `-d` option specifies the distro to use. This must be one of the
Evergreen-recognized distros. The arguments are the environment variables as
would be set by Evergreen configuration (i.e. `config.yml` in this directory).
All arguments are optional.

By default the entire test suite is run (using mlaunch to launch the server);
to specify another script, use `-s` option:

    ./.evergreen/test-on-docker -s .evergreen/run-tests-kerberos-unit.sh

To override just the test command (but maintain the setup performed
by Evergreen shell scripts), use TEST_CMD:

    ./.evergreen/test-on-docker TEST_CMD='rspec spec/mongo/auth'

### Toolchain and Server Preloading

The docker test runner supports preloading Ruby interpreters and server
binaries in the docker image, which reduces the runtime of subsequent
test runs. To turn on preloading, use `-p` option:

    ./.evergreen/test-on-docker -p

It is possible to run the test suite offline (without Internet access)
provided the full process has already been executed. This is accomplished
with the `-e` option and only makes sense when `-p` is also used:

    ./.evergreen/test-on-docker -pe

### Private Environment Variables

Normally the environment variables are specified on the command line as
positional arguments. However the Ruby driver Evergreen projects also have
private variables containing various passwords which are not echoed in the
build logs, and therefore are not conveniently providable using the normal
environment variable handling.

Instead, these variables can be collected into a
[.env](https://github.com/bkeepers/dotenv)-compatible configuration file,
and the path to this configuration file can be provided via the `-a`
option to the test runner. The `-a` option may be given multiple times.

When creating the .env files from Evergreen private variables, the variable
names must be uppercased.

For example, to execute Kerberos integration tests which require private
variables pertanining to the test Kerberos server, you could run:

    ./.evergreen/test-on-docker -d rhel70 RVM_RUBY=ruby-2.5 \
      -s .evergreen/run-tests-kerberos-integration.sh -pa .env.private

The `.env.private` path specifically is listed in .gitignore and .dockerignore
files, and is thus ignored by both Git and Docker.

The private environment variables provided via the `-a` argument are
specified in the `docker run` invocation and are not part of the image
created by `docker build`. Because of this, they override any environment
variables provided as positional arguments.

### Field-Level Encryption FLE

The Docker testing script supports running tests with field-level encryption (FLE).
To enable FLE, set the FLE environment variable to true.

Some FLE tests require other environment variables to be set as well. You may
specify these environment variables in a private .env file as explained in the
[Private Environment Variables](#private-environment-variables) section.

The following is a list of required environment variables:
- MONGO_RUBY_DRIVER_AWS_KEY
- MONGO_RUBY_DRIVER_AWS_SECRET
- MONGO_RUBY_DRIVER_AWS_REGION
- MONGO_RUBY_DRIVER_AWS_ARN
- MONGO_RUBY_DRIVER_AZURE_TENANT_ID
- MONGO_RUBY_DRIVER_AZURE_CLIENT_ID
- MONGO_RUBY_DRIVER_AZURE_CLIENT_SECRET
- MONGO_RUBY_DRIVER_AZURE_IDENTITY_PLATFORM_ENDPOINT
- MONGO_RUBY_DRIVER_AZURE_KEY_VAULT_ENDPOINT
- MONGO_RUBY_DRIVER_AZURE_KEY_NAME
- MONGO_RUBY_DRIVER_GCP_EMAIL
- MONGO_RUBY_DRIVER_GCP_PRIVATE_KEY

Here's an example of how to run FLE tests in Docker:

  ./.evergreen/test-on-docker FLE=true -pa .env.private

### rhel62

To run rhel62 distro in docker, host system must be configured to [emulate
syscalls](https://github.com/CentOS/sig-cloud-instance-images/issues/103).
Note that this defeats one of the patches for the Spectre set of processor
vulnerabilities.


## Running MongoDB Server In Docker

It is possible to use the Docker infrastructure provided by the test suite
to provision a MongoDB server deployment in Docker and expose it to the host.
Doing so allows testing on all server versions supported by the test suite
without having to build and install them on the host system, as well as
running the deployment on a distro that differs from that of the host system.

To provision a deployment, use the `-m` option. This option requires one
argument which is the port number on the host system to use as the starting
port for the deployment. Use the Evergreen environment variable syntax to
specify the desired server version, topology, authentication and other
parameters. The `-p` argument is supported to preload the server into the
Docker image and its use is recommended with `-m`.

To run a standalone server and expose it on the default port, 27017:

    ./.evergreen/test-on-docker -pm 27017

To run a replica set deployment with authentication and expose its members
on ports 30000 through 30002:

    ./.evergreen/test-on-docker -pm 30000 -d debian92 TOPOLOGY=replica-set AUTH=auth

When OCSP is enabled, the test OCSP responder will be launched on port 8100
and this port will be exposed to the host OS. There must not be another service
using this port on the host OS.


## Testing in AWS

The scripts described in this section assist in running the driver test suite
on EC2 instances and in ECS tasks.

It is recommended to test via Docker on EC2 instances, as this produces
shorter test cycles since all of the cleanup is handled by Docker.
Docker is not usable on ECS (because ECS tasks are already running in
Docker themselves), thus to test in ECS tasks it is required to use
non-Docker scripts which generally rebuild more of the target instance and
thus have longer test cycles.

### Instance Types

The test suite, as well as the Docker infrastructure if it is used,
require a decent amount of memory to run. Starting with 2 GB generally
works well, for example via the `t3a.small` instance type.

### Supported Operating Systems

Currently Debian and Ubuntu operating systems are supported. Support for
other operating systems may be added in the future.

### `ssh-agent` Setup

The AWS testing scripts do not provide a way to specify the private key
to use for authentication. This functionality is instead delegated to
`ssh-agent`. If you do not already have it configured, you can run from
your shell:

    eval `ssh-agent`

This launches a `ssh-agent` instance for the shell in which you run this
command. It is more efficient to run a single `ssh-agent` for the entire
machine but the procedure for setting this up is outside the scope of this
readme file.

With the agent running, add the private key corresponding to the key pair
used to launch the EC2 instance you wish to use for testing:

    ssh-add path/to/key-pair.pem

### Provision

Given an EC2 instance running a supported Debian or Ubuntu version at
IP `12.34.56.78`, use the `provision-remote` command to prepare it for
being used to run the driver's test suite. This command takes two arguments:
the target, in the form of `username@ip`, and the type of provisioning
to perform which can be `docker` or `local`. Note that the username for
Debian instances is `admin` and the username for Ubuntu instances is `ubuntu`:

    # Configure a Debian instance to run the test suite via Docker
    ./.evergreen/provision-remote admin@12.34.56.78 docker

    # Configure an Ubuntu instance to run the test suite without Docker
    ./.evergreen/provision-remote ubuntu@12.34.56.78 local

This only needs to be done once per instance.

### Run Tests - Docker

When testing on an EC2 instance, it is recommended to run the tests via Docker
In this scenario a docker image is created on the EC2 instance with appropriate
configuration, then a container is run using this image which executes the
test suite. All parameters supported by the Docker test script described
above are supported.

Note that the private environment files (`.env.private*`), if any exist,
are copied to the EC2 instance. This is done so that, for example, AWS auth
may be tested in EC2 which generally requires private environment variables.

Run the `test-docker-remote` script as follows:

    ./.evergreen/test-docker-remote ubuntu@12.34.56.78 MONGODB_VERSION=4.2 -p

The first argument is the target on which to run the tests. All subsequent
arguments are passed to the `test-on-docker` script. In this case, `test-docker-remote`
will execute the following script on the target instance:

    ./.evergreen/test-on-docker MONGODB_VERSION=4.2 -p

All arguments that `test-on-docker` accepts are accepted by `test-docker-remote`.
For example, to verify that all of the tooling is working correctly but not
run any tests you could issue;

    ./.evergreen/test-on-docker -p TEST_CMD=true

The private environment files need to be specified explicitly, just like they
need to be explicitly specified to `test-on-docker`. For example:

    ./.evergreen/test-on-docker MONGODB_VERSION=4.2 -pa .env.private

### Run Tests - Local

When testing in an ECS task, the only option is to execute the test suite
locally to the task. This strategy can also be used on an EC2 instance,
although this is not recommended because the test cycle is longer compared
to the Docker testing strategy.

To run the tests in the task, use the `test-remote` script as follows:

    ./.evergreen/test-remote ubuntu@12.34.56.78 \
      env MONGODB_VERSION=4.4 AUTH=aws-regular .evergreen/run-tests-aws-auth.sh

The first argument is the target in the `username@ip` format. The script
first copies the current directory to the target, then executes the remaining
arguments as a shell command on the target. This example uses `env` to set
environment variables that are referenced by the `.evergreen/run-tests-aws-auth.sh`
script.
