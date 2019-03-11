require 'lite_spec_helper'

# Replica set name can be overridden via replicaSet parameter in MONGODB_URI
# environment variable or by specifying RS_NAME environment variable when
# not using MONGODB_URI.
TEST_SET = 'ruby-driver-rs'

require 'support/authorization'
require 'support/primary_socket'
require 'support/constraints'
require 'support/cluster_config'
require 'support/cluster_tools'
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

# require all shared examples
Dir['./spec/support/shared/*.rb'].sort.each { |file| require file }
