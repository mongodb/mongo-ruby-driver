# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

# This test repeatedly creates and closes clients across several threads.
# Its goal is to ensure that the push monitor connections specifically get
# closed without any errors or warnings being reported to applications.
#
# Although the test is specifically meant to test 4.4+ servers (that utilize
# the push monitor) in non-LB connections, run it everywhere for good measure.
describe 'Push monitor close test' do
  require_stress

  let(:options) do
    SpecConfig.instance.all_test_options
  end

  before(:all) do
    # load if necessary
    ClusterConfig.instance.primary_address
    ClientRegistry.instance.close_all_clients
  end

  it 'does not warn/error on cleanup' do
    Mongo::Logger.logger.should_not receive(:warn)

    threads = 10.times.map do
      Thread.new do
        10.times do
          client = new_local_client([ClusterConfig.instance.primary_address.seed], options)
          if rand > 0.33
            client.command(ping: 1)
            sleep(rand * 3)
          end
          client.close
          STDOUT << '.'
        end
      end
    end
    threads.each(&:join)
    puts
  end
end
