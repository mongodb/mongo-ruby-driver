# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

require 'runners/sdam'
require 'runners/sdam/verifier'

describe 'SDAM Monitoring' do
  include Mongo::SDAM

  SDAM_MONITORING_TESTS.each do |file|

    spec = Mongo::SDAM::Spec.new(file)

    context("#{spec.description} (#{file.sub(%r'.*/data/sdam_monitoring/', '')})") do

      before(:all) do
        @subscriber = Mrss::PhasedEventSubscriber.new
        sdam_proc = lambda do |client|
          client.subscribe(Mongo::Monitoring::SERVER_OPENING, @subscriber)
          client.subscribe(Mongo::Monitoring::SERVER_CLOSED, @subscriber)
          client.subscribe(Mongo::Monitoring::SERVER_DESCRIPTION_CHANGED, @subscriber)
          client.subscribe(Mongo::Monitoring::TOPOLOGY_OPENING, @subscriber)
          client.subscribe(Mongo::Monitoring::TOPOLOGY_CHANGED, @subscriber)
        end
        @client = new_local_client_nmio(spec.uri_string,
          sdam_proc: sdam_proc,
          heartbeat_frequency: 100, connect_timeout: 0.1)
        # We do not want to create servers when an event referencing them
        # is processed, because this may result in server duplication
        # when events are processed for servers that had been removed
        # from the topology. Instead set up a server cache we can use
        # to reference servers removed from the topology
        @servers_cache = {}
        @client.cluster.servers_list.each do |server|
          @servers_cache[server.address.to_s] = server

          # Since we set monitoring_io: false, servers are not monitored
          # by the cluster. Start monitoring on them manually (this publishes
          # the server opening event but, again due to monitoring_io being
          # false, does not do network I/O or change server status).
          #
          # If the server is a load balancer, it doesn't normally get monitored
          # so don't start here either.
          unless server.load_balancer?
            server.start_monitoring
          end
        end
      end

      after(:all) do
        @client.close
      end

      spec.phases.each_with_index do |phase, phase_index|

        context("Phase: #{phase_index + 1}") do

          before(:all) do
            phase.responses&.each do |response|
              # For each response in the phase, we need to change that server's description.
              server = find_server(@client, response.address)
              server ||= @servers_cache[response.address.to_s]
              if server.nil?
                raise "Server should have been found"
              end

              result = response.hello
              # Spec tests do not always specify wire versions, but the
              # driver requires them. Set them to zero which was
              # the legacy default in the driver.
              result['minWireVersion'] ||= 0
              result['maxWireVersion'] ||= 0
              new_description = Mongo::Server::Description.new(
                server.description.address, result, average_round_trip_time: 0.5)
              @client.cluster.run_sdam_flow(server.description, new_description)
            end
            @subscriber.phase_finished(phase_index)
          end

          it "expects #{phase.outcome.events.length} events to be published" do
            expect(@subscriber.phase_events(phase_index).length).to eq(phase.outcome.events.length)
          end

          let(:verifier) do
            Sdam::Verifier.new
          end

          phase.outcome.events.each_with_index do |expectation, index|

            it "expects event #{index+1} to be #{expectation.name}" do
              verifier.verify_sdam_event(
                phase.outcome.events, @subscriber.phase_events(phase_index), index)
            end
          end
        end
      end
    end
  end
end
