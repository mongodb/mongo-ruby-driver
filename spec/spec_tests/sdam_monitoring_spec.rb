require 'lite_spec_helper'

describe 'SDAM Monitoring' do
  include Mongo::SDAM

  SDAM_MONITORING_TESTS.each do |file|

    spec = Mongo::SDAM::Spec.new(file)

    context("#{spec.description} (#{file.sub(%r'.*/data/sdam_monitoring/', '')})") do

      before(:all) do
        @subscriber = Mongo::SDAMMonitoring::PhasedTestSubscriber.new
        sdam_proc = lambda do |client|
          client.subscribe(Mongo::Monitoring::SERVER_OPENING, @subscriber)
          client.subscribe(Mongo::Monitoring::SERVER_CLOSED, @subscriber)
          client.subscribe(Mongo::Monitoring::SERVER_DESCRIPTION_CHANGED, @subscriber)
          client.subscribe(Mongo::Monitoring::TOPOLOGY_OPENING, @subscriber)
          client.subscribe(Mongo::Monitoring::TOPOLOGY_CHANGED, @subscriber)
        end
        @client = Mongo::Client.new(spec.uri_string,
          sdam_proc: sdam_proc, monitoring_io: false,
          heartbeat_frequency: 100, connect_timeout: 0.1)
        # We do not want to create servers when an event referencing them
        # is processed, because this may result in server duplication
        # when events are processed for servers that had been removed
        # from the topology. Instead set up a server cache we can use
        # to reference servers removed from the topology
        @servers_cache = {}
        @client.cluster.servers_list.each do |server|
          @servers_cache[server.address.to_s] = server
        end
      end

      after(:all) do
        @client.close
      end

      spec.phases.each_with_index do |phase, phase_index|

        context("Phase: #{phase_index + 1}") do

          before(:all) do
            phase.responses.each do |response|
              # For each response in the phase, we need to change that server's description.
              server = find_server(@client, response.address)
              server ||= @servers_cache[response.address.to_s]
              if server.nil?
                raise "Server should have been found"
              end

              result = response.ismaster
              # Spec tests do not always specify wire versions, but the
              # driver requires them. Set them to zero which was
              # the legacy default in the driver.
              result['minWireVersion'] ||= 0
              result['maxWireVersion'] ||= 0
              new_description = Mongo::Server::Description.new(
                server.description.address, result, 0.5)
              publisher = SdamSpecEventPublisher.new(@client.cluster.send(:event_listeners))
              publisher.publish(Mongo::Event::DESCRIPTION_CHANGED, server.description, new_description)
            end
            @subscriber.phase_finished(phase_index)
          end

          it "expects #{phase.outcome.events.length} events to be published" do
            expect(@subscriber.phase_events(phase_index).length).to eq(phase.outcome.events.length)
          end

          phase.outcome.events.each_with_index do |expectation, index|

            it "expects a #{expectation.name} to be published" do
              published_event = @subscriber.phase_events(phase_index)[index]
              expect(published_event).not_to be_nil
              expect(published_event).to match_sdam_monitoring_event(expectation)
            end
          end
        end
      end
    end
  end
end
