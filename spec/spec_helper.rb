require 'lite_spec_helper'

# Replica set name can be overridden via replicaSet parameter in MONGODB_URI
# environment variable or by specifying RS_NAME environment variable when
# not using MONGODB_URI.
TEST_SET = 'ruby-driver-rs'

require 'support/travis'
require 'support/authorization'
require 'support/primary_socket'
require 'support/constraints'
require 'rspec/retry'

RSpec.configure do |config|
  config.include(Authorization)
  config.extend(Constraints)

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
  SpecConfig.instance.addresses.size == 1
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

# Is the test suite running locally (not on Travis).
#
# @since 2.1.0
def testing_ssl_locally?
  SpecConfig.instance.ssl? && !(ENV['CI'])
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
      e.message =~ /(not authorized)|(unauthorized)|(no users authenticated)|(requires authentication)/
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
  Mongo::Client.new(SpecConfig.instance.addresses, TEST_OPTIONS.merge(database: TEST_DB))
end

# Converts a 'camelCase' string or symbol to a :snake_case symbol.
def camel_to_snake(ident)
  ident = ident.is_a?(String) ? ident.dup : ident.to_s
  ident[0] = ident[0].downcase
  ident.chars.reduce('') { |s, c| s + (/[A-Z]/ =~ c ? "_#{c.downcase}" : c) }.to_sym
end

# Creates a copy of a hash where all keys and string values are converted to snake-case symbols.
# For example, `{ 'fooBar' => { 'baz' => 'bingBing', :x => 1 } }` converts to
# `{ :foo_bar => { :baz => :bing_bing, :x => 1 } }`.
def snakeize_hash(value)
  return camel_to_snake(value) if value.is_a?(String)
  return value unless value.is_a?(Hash)

  value.reduce({}) do |hash, kv|
    hash.tap do |h|
      h[camel_to_snake(kv.first)] = snakeize_hash(kv.last)
    end
  end
end

# require all shared examples
Dir['./spec/support/shared/*.rb'].sort.each { |file| require file }
