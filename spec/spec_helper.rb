require 'lite_spec_helper'

require 'support/authorization'
require 'support/primary_socket'
require 'support/constraints'
require 'support/cluster_config'
require 'support/cluster_tools'
require 'rspec/retry'
require 'support/monitoring_ext'
require 'support/local_resource_registry'

RSpec.configure do |config|
  config.include(Authorization)
  config.extend(Constraints)

  config.before(:all) do
    if ClusterConfig.instance.fcv_ish >= '3.6'
      kill_all_server_sessions
    end
  end

  config.after do
    LocalResourceRegistry.instance.close_all
    ClientRegistry.instance.close_local_clients
  end
end

# require all shared examples
Dir['./spec/support/shared/*.rb'].sort.each { |file| require file }
