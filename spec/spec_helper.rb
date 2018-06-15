# The tests run against a MongoDB cluster which is
# configured and started externally to the test suite. This allows
# running the entire test suite against, for example, a standalone
# mongod as well as a replica set. The flip side to this is the
# test suite will not work without a running mongo cluster, and
# tests which are not applicable to or cannot be performed on the
# running mongo cluster are skipped.
#
# Not only does the test suite require an externally launched cluster,
# the test suite must also be told how the cluster is configured
# via MONGODB_URI, TOPOLOGY, MONGODB_ADDRESSES, RS_ENABLED, RS_NAME and
# SHARDED_ENABLED environment variables.
#
# The test suite does not validate that it is able to successfully connect
# to the cluster prior to running the tests. If a connection fails entirely,
# the clue is generally failures to invoke methods on nil.
# However, it is also possible to establish a connection to a cluster which
# is not quite correctly configured. The result is usually a mass of test
# failures that are indistinguishable from legitimate failures.
#
# Additionally some of the tests assume that the seed list (given in
# MONGODB_URI or MONGODB_ADDRESSES) encompasses all servers in the cluster,
# and will fail when MONGODB_URI includes only one host of a replica set.
# It is best to include all hosts of the cluster in MONGODB_URI and
# MONGODB_ADDRESSES.
#
# The test suite seems to have issues connecting to a replica set
# via IP addresses if the replica set hosts are defined with hostnames
# (i.e., 127.0.0.1 vs localhost). Try to exactly match the contents of
# MONGODB_URI and `rs.isMaster()` output, either by adjusting MONGODB_URI
# or by reconfiguring the replica set.
#
# In order to run spec tests, the mongo cluster needs to have failpoints
# enabled. This is accomplished by starting mongod with the following option:
#   --setParameter enableTestCommands=1
#
# Use the following environment variables to configure the tests:
#
# CLIENT_DEBUG: Show debug messages from the client.
#   CLIENT_DEBUG=1
#
# MONGODB_URI: Connection string to use. This must be a valid MongoDB URI;
# mongodb:// and mongodb+srv:// are both supported.
# RS_ENABLED and SHARDED_ENABLED are NOT honored if using MONGODB_URI -
# specify replica set name in the URI and to specify a sharded topology
# set TOPOLOGY=sharded_cluster environment variable.
#   MONGODB_URI=mongodb://127.0.0.1:27001/?replicaSet=test
#   MONGODB_URI=mongodb://127.0.0.1:27001,127.0.0.1:27002/ TOPOLOGY=sharded_cluster
#
# MONGODB_ADDRESSES: Specify addresses to connect to. Use RS_ENABLED,
# RS_NAME and SHARDED_ENABLED to configure the topology.
#   MONGODB_ADDRESSES=127.0.0.1:27017,127.0.0.1:27018
#   MONGODB_ADDRESSES=127.0.0.1:27017,127.0.0.1:27018 RS_ENABLED=1
#   MONGODB_ADDRESSES=127.0.0.1:27017,127.0.0.1:27018 RS_ENABLED=1 RS_NAME=test
#   MONGODB_ADDRESSES=127.0.0.1:27017,127.0.0.1:27018 SHARDED_ENABLED=1
#
# RS_ENABLED: Instruct the test suite to connect to a replica set.
# RS_ENABLED is only honored when not using MONGODB_URI; to connect to a
# replica set with MONGODB_URI, specify the replica set name in the URI
# (despite the Ruby driver performing topology discovery by default, it
# doesn't do so in the test suite).
# RS_NAME can be given to specify the replica set name; the default is
# ruby-driver-rs.
#   RS_ENABLED=1
#   RS_ENABLED=1 RS_NAME=test
#
# SHARDED_ENABLED: Instruct the test suite to connect to the sharded cluster.
# Set MONGODB_URI appropriately as well.
#   SHARDED_ENABLED=1

require 'lite_spec_helper'

# Replica set name can be overridden via replicaSet parameter in MONGODB_URI
# environment variable or by specifying RS_NAME environment variable when
# not using MONGODB_URI.
TEST_SET = 'ruby-driver-rs'

require 'support/travis'
require 'support/authorization'

RSpec.configure do |config|
  config.include(Authorization)

  config.before(:suite) do
    begin
      # Create the root user administrator as the first user to be added to the
      # database. This user will need to be authenticated in order to add any
      # more users to any other databases.
      ADMIN_UNAUTHORIZED_CLIENT.database.users.create(ROOT_USER)
      ADMIN_UNAUTHORIZED_CLIENT.close
    rescue Exception => e
    end
    begin
      # Adds the test user to the test database with permissions on all
      # databases that will be used in the test suite.
      ADMIN_AUTHORIZED_TEST_CLIENT.database.users.create(TEST_USER)
    rescue Exception => e
    end
  end
end

# Determine whether the test clients are connecting to a standalone.
#
# @since 2.0.0
def standalone?
  $mongo_client ||= initialize_scanned_client!
  $standalone ||= $mongo_client.cluster.servers.first.standalone?
end

# Determine whether the test clients are connecting to a replica set.
#
# @since 2.0.0
def replica_set?
  $mongo_client ||= initialize_scanned_client!
  $replica_set ||= $mongo_client.cluster.replica_set?
end

# Determine whether the test clients are connecting to a sharded cluster
# or a single mongos.
#
# @since 2.0.0
def sharded?
  $mongo_client ||= initialize_scanned_client!
  $sharded ||= ($mongo_client.cluster.sharded? || single_mongos?)
end

# Determine whether the single address provided is a replica set member.
# @note To run the specs relying on this to return true,
#   start a replica set and set the environment variable
#   MONGODB_ADDRESSES to the address of a single member.
#
# @since 2.0.0
def single_rs_member?
  $mongo_client ||= initialize_scanned_client!
  $single_rs_member ||= (single_seed? &&
      $mongo_client.cluster.servers.first.replica_set_name)
end

# Determine whether the single address provided is a mongos.
# @note To run the specs relying on this to return true,
#   start a sharded cluster and set the environment variable
#   MONGODB_ADDRESSES to the address of a single mongos.
#
# @since 2.0.0
def single_mongos?
  $mongo_client ||= initialize_scanned_client!
  $single_mongos ||= (single_seed? &&
      $mongo_client.cluster.servers.first.mongos?)
end

# Determine whether a single address was provided.
#
# @since 2.0.0
def single_seed?
  ADDRESSES.size == 1
end

# For instances where behaviour is different on different versions, we need to
# determine in the specs if we are 3.6 or higher.
#
# @since 2.5.0
def op_msg_enabled?
  $mongo_client ||= initialize_scanned_client!
  $op_msg_enabled ||= $mongo_client.cluster.servers.first.features.op_msg_enabled?
end
alias :change_stream_enabled? :op_msg_enabled?
alias :sessions_enabled? :op_msg_enabled?


# Whether sessions can be tested. Sessions are available on server versions 3.6
#   and higher and when connected to a replica set or sharded cluster.
#
# @since 2.5.0
def test_sessions?
  sessions_enabled? && (replica_set? || sharded?)
end

# Whether change streams can be tested. Change streams are available on server versions 3.6
#   and higher and when connected to a replica set.
#
# @since 2.5.0
def test_change_streams?
  !BSON::Environment.jruby? && change_stream_enabled? & replica_set?
end

# Whether transactions can be tested. Transactions are available on server versions 4.0 and higher
#   and when connected to a replica set.
#
# @since 2.6.0
def test_transactions?
  transactions_enabled? && replica_set?
end

# For instances where behaviour is different on different versions, we need to
# determine in the specs if we are 3.6 or higher.
#
# @since 2.5.0
def array_filters_enabled?
  $mongo_client ||= initialize_scanned_client!
  $array_filters_enabled ||= $mongo_client.cluster.servers.first.features.array_filters_enabled?
end


# For instances where behaviour is different on different versions, we need to
# determine in the specs if we are 3.4 or higher.
#
# @since 2.4.0
def collation_enabled?
  $mongo_client ||= initialize_scanned_client!
  $collation_enabled ||= $mongo_client.cluster.servers.first.features.collation_enabled?
end

# For instances where behaviour is different on different versions, we need to
# determine in the specs if we are 3.2 or higher.
#
# @since 2.0.0
def find_command_enabled?
  $mongo_client ||= initialize_scanned_client!
  $find_command_enabled ||= $mongo_client.cluster.servers.first.features.find_command_enabled?
end

# For instances where behaviour is different on different versions, we need to
# determine in the specs if we are 2.7 or higher.
#
# @since 2.0.0
def list_command_enabled?
  $mongo_client ||= initialize_scanned_client!
  $list_command_enabled ||= $mongo_client.cluster.servers.first.features.list_indexes_enabled?
end

# For instances where behavior is different on different versions, we need to
# determine in the specs if we are 4.0 or higher.
#
# @since 2.6.0
def scram_sha_256_enabled?
  $mongo_client ||= initialize_scanned_client!
  $scram_sha_256_enabled ||= $mongo_client.cluster.servers.first.features.scram_sha_256_enabled?
end

alias :transactions_enabled? :scram_sha_256_enabled?

# Is the test suite running locally (not on Travis).
#
# @since 2.1.0
def testing_ssl_locally?
  running_ssl? && !(ENV['CI'])
end

# Should tests relying on external connections be run.
#
# @since 2.5.1
def test_connecting_externally?
  !ENV['CI'] && !ENV['EXTERNAL_DISABLED']
end

# Is the test suite running on SSL.
#
# @since 2.0.2
def running_ssl?
  SSL
end

# Is the test suite using compression.
#
# @since 2.5.0
def compression_enabled?
  COMPRESSORS[:compressors]
end

# Is the test suite testing compression.
# Requires that the server supports compression and compression is used by the test client.
#
# @since 2.5.0
def testing_compression?
  compression_enabled? && op_msg_enabled?
end

alias :scram_sha_1_enabled? :list_command_enabled?

# Try running a command on the admin database to see if the mongod was started with auth.
#
# @since 2.2.0
def auth_enabled?
  if auth = ENV['AUTH']
    auth == 'auth'
  else
    $mongo_client ||= initialize_scanned_client!
    begin
      $mongo_client.use(:admin).command(getCmdLineOpts: 1).first["argv"].include?("--auth")
    rescue => e
      e.message =~ /(not authorized)|(unauthorized)/
    end
  end
end

def need_to_skip_on_sharded_auth_40?
  sharded? && auth_enabled? && scram_sha_256_enabled?
end

# Can the driver specify a write concern that won't be overridden? (mongos 4.0+ overrides the write
# concern)
#
# @since 2.6.0
def can_set_write_concern?
  !sharded? || !scram_sha_256_enabled?
end

# Initializes a basic scanned client to do an ismaster check.
#
# @since 2.0.0
def initialize_scanned_client!
  Mongo::Client.new(ADDRESSES, TEST_OPTIONS.merge(database: TEST_DB))
end

# require all shared examples
Dir['./spec/support/shared/*.rb'].sort.each { |file| require file }
