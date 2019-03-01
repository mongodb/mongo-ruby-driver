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

  config.before(:all) do
    if ClusterConfig.instance.fcv_ish >= '3.6'
      kill_all_server_sessions
    end
  end
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
