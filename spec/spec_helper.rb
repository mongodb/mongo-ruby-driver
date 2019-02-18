require 'lite_spec_helper'

# Replica set name can be overridden via replicaSet parameter in MONGODB_URI
# environment variable or by specifying RS_NAME environment variable when
# not using MONGODB_URI.
TEST_SET = 'ruby-driver-rs'

require 'support/authorization'
require 'support/primary_socket'
require 'support/constraints'
require 'support/cluster_config'
require 'rspec/retry'
require 'support/monitoring_ext'

RSpec.configure do |config|
  config.include(Authorization)
  config.extend(Constraints)

  config.after(:each) do
    if rand < 0.01
      close_local_clients
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
  ClusterConfig.instance.single_server? && ClusterConfig.instance.replica_set_name
end

# Determine whether the single address provided is a mongos.
# @note To run the specs relying on this to return true,
#   start a sharded cluster and set the environment variable
#   MONGODB_ADDRESSES to the address of a single mongos.
#
# @since 2.0.0
def single_mongos?
  ClusterConfig.instance.single_server? && ClusterConfig.instance.mongos?
end

# For instances where behavior is different on different versions, we need to
# determine in the specs if we are 3.6 or higher.
#
# @since 2.5.0
def op_msg_enabled?
  $op_msg_enabled ||= scanned_client_server!.features.op_msg_enabled?
end
alias :change_stream_enabled? :op_msg_enabled?

# For instances where behavior is different on different versions, we need to
# determine in the specs if we are 3.4 or higher.
#
# @since 2.4.0
def collation_enabled?
  $collation_enabled ||= scanned_client_server!.features.collation_enabled?
end

# For instances where behavior is different on different versions, we need to
# determine in the specs if we are 3.2 or higher.
#
# @since 2.0.0
def find_command_enabled?
  $find_command_enabled ||= scanned_client_server!.features.find_command_enabled?
end

# For instances where behavior is different on different versions, we need to
# determine in the specs if we are 2.7 or higher.
#
# @since 2.0.0
def list_command_enabled?
  $list_command_enabled ||= scanned_client_server!.features.list_indexes_enabled?
end

# For instances where behavior is different on different versions, we need to
# determine in the specs if we are 4.0 or higher.
#
# @since 2.6.0
def scram_sha_256_enabled?
  $scram_sha_256_enabled ||= scanned_client_server!.features.scram_sha_256_enabled?
end

# Is the test suite using compression.
#
# @since 2.5.0
def compression_enabled?
  !SpecConfig.instance.compressors.nil?
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
  ClientRegistry.instance.global_client('basic')
end

class ScannedClientHasNoServers < StandardError; end

def scanned_client_server!
  $mongo_client ||= initialize_scanned_client!
  server = $mongo_client.cluster.servers.first
  if server.nil?
    raise ScannedClientHasNoServers
  end
  server
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
