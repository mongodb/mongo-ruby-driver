require 'lite_spec_helper'

describe 'SDAM Monitoring' do
  include Mongo::SDAM

  SDAM_MONITORING_TESTS.each do |file|

    spec = Mongo::SDAM::Spec.new(file)

    context("#{spec.description} (#{file.sub(%r'.*support/sdam_monitoring/', '')})") do

      before(:all) do
        @subscriber = Mongo::SDAMMonitoring::TestSubscriber.new
        sdam_proc = lambda do |client|
          client.subscribe(Mongo::Monitoring::SERVER_OPENING, @subscriber)
          client.subscribe(Mongo::Monitoring::SERVER_CLOSED, @subscriber)
          client.subscribe(Mongo::Monitoring::SERVER_DESCRIPTION_CHANGED, @subscriber)
          client.subscribe(Mongo::Monitoring::TOPOLOGY_OPENING, @subscriber)
          client.subscribe(Mongo::Monitoring::TOPOLOGY_CHANGED, @subscriber)
        end
        @client = Mongo::Client.new(spec.uri_string,
          sdam_proc: sdam_proc,
          heartbeat_frequency: 100, connect_timeout: 0.1)
      end

      after(:all) do
        @client.close
      end

      spec.phases.each_with_index do |phase, index|

        context("Phase: #{index + 1}") do

          before(:all) do
            phase.responses.each do |response|
              # For each response in the phase, we need to change that server's description.
              server = find_server(@client, response.address)
              server ||= Mongo::Server.new(
                           Mongo::Address.new(response.address),
                           @client.cluster,
                           @client.send(:monitoring),
                           @client.cluster.send(:event_listeners),
                           @client.cluster.options
                         )

              monitor = server.instance_variable_get(:@monitor)
              description = monitor.inspector.run(server.description, response.ismaster, 0.5)
              monitor.instance_variable_set(:@description, description)
            end
          end

          phase.outcome.events.each do |expectation|

            it "expects a #{expectation.name} to be fired" do
              fired_event = @subscriber.first_event(expectation.name)
              expect(fired_event).not_to be_nil
              expect(fired_event).to match_sdam_monitoring_event(expectation)
            end
          end
        end
      end
    end
  end
end
