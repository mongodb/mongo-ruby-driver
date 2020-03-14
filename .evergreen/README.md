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

    ./.evergreen/test-on-docker -s .evergreen/run-tests-enterprise-auth.sh

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

For example, to execute enterprise auth tests which require private variables
pertanining to the test Kerberos server, you could run:

    ./.evergreen/test-on-docker -d rhel70 RVM_RUBY=ruby-2.3 \
      -s .evergreen/run-tests-enterprise-auth.sh -pa .env.private

The `.env.private` path specifically is listed in .gitignore and .dockerignore
files, and is thus ignored by both Git and Docker.

### rhel62

To run rhel62 distro in docker, host system must be configured to [emulate
syscalls](https://github.com/CentOS/sig-cloud-instance-images/issues/103).
Note that this defeats one of the patches for the Spectre set of processor
vulnerabilities.

## Running Deployment In Docker

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
