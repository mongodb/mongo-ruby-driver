# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

require 'mrss/constraints'
require 'mrss/cluster_config'

ClusterConfig = Mrss::ClusterConfig

require 'support/constraints'
require 'support/authorization'
require 'support/primary_socket'
require 'support/cluster_tools'
require 'support/monitoring_ext'

RSpec.configure do |config|
  config.include(Authorization)
  config.extend(Mrss::Constraints)
  config.extend(Constraints)

  config.before(:all) do
    if SpecConfig.instance.kill_all_server_sessions?
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
