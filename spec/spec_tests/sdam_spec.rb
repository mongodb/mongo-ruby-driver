require 'lite_spec_helper'

describe 'Server Discovery and Monitoring' do
  include Mongo::SDAM

  SERVER_DISCOVERY_TESTS.each do |file|

    spec = Mongo::SDAM::Spec.new(file)

    context("#{spec.description} (#{file.sub(%r'.*/data/sdam/', '')})") do
      before(:all) do
        class Mongo::Server::Monitor
          alias_method :run_saved!, :run!

          # Replace run! method to do nothing, to avoid races between
          # the background thread started by Server.new and our mocking.
          # Replace with refinements once ruby 1.9 support is dropped
          def run!
          end
        end
      end

      after(:all) do
        class Mongo::Server::Monitor
          alias_method :run!, :run_saved!
        end
      end

      before(:all) do
        # Since we supply all server descriptions and drive events,
        # background monitoring only gets in the way. Disable it.
        @client = Mongo::Client.new(spec.uri_string, monitoring_io: false)
        client_options = @client.instance_variable_get(:@options)
        @client.instance_variable_set(:@options, client_options.merge(heartbeat_frequency: 100, connect_timeout: 0.1))
        @client.cluster.instance_variable_set(:@options, client_options.merge(heartbeat_frequency: 100, connect_timeout: 0.1))
      end

      after(:all) do
        @client && @client.close
      end

      spec.phases.each_with_index do |phase, index|

        context("Phase: #{index + 1}") do

          before do
            allow(@client.cluster).to receive(:connected?).and_return(true)

            phase.responses.each do |response|
              server = find_server(@client, response.address)
              unless server
                server = Mongo::Server.new(
                    Mongo::Address.new(response.address),
                    @client.cluster,
                    @client.send(:monitoring),
                    @client.cluster.send(:event_listeners),
                    @client.cluster.options,
                )
              end
              monitor = server.instance_variable_get(:@monitor)
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
          end

          if phase.outcome.compatible?

            let(:cluster_addresses) do
              @client.cluster.instance_variable_get(:@servers).
                  collect(&:address).collect(&:to_s).uniq.sort
            end

            let(:phase_addresses) do
              phase.outcome.servers.keys.sort
            end

            it "sets the cluster topology to #{phase.outcome.topology_type}" do
              expect(@client.cluster).to be_topology(phase.outcome.topology_type)
            end

            it "sets the cluster replica set name to #{phase.outcome.set_name.inspect}" do
              expect(@client.cluster.replica_set_name).to eq(phase.outcome.set_name)
            end

            it "sets the cluster logical session timeout minutes to #{phase.outcome.logical_session_timeout.inspect}" do
              expect(@client.cluster.logical_session_timeout).to eq(phase.outcome.logical_session_timeout)
            end

            it "has the expected servers in the cluster" do
              expect(cluster_addresses).to eq(phase_addresses)
            end

            # If compatible is not expliticly specified in the fixture,
            # wire protocol versions aren't either and the topology
            # is actually incompatible
            if phase.outcome.compatible_specified?
              it 'is compatible' do
                expect(@client.cluster.topology.compatible?).to be true
              end
            end

            phase.outcome.servers.each do |address_str, server_spec|

              it "sets #{address_str} server to #{server_spec['type']}" do
                server = find_server(@client, address_str)
                unless server_of_type?(server, server_spec['type'])
                  raise RSpec::Expectations::ExpectationNotMetError,
                    "Server #{server.summary} not of type #{server_spec['type']}"
                end
              end

              it "sets #{address_str} server replica set name to #{server_spec['setName'].inspect}" do
                expect(find_server(@client, address_str).replica_set_name).to eq(server_spec['setName'])
              end

              it "sets #{address_str} server description in topology to match server description in cluster" do
                desc = @client.cluster.topology.server_descriptions[address_str]
                server = find_server(@client, address_str)
                # eql doesn't work here because it's aliased to eq
                # and two unknowns are not eql as a result,
                # compare by object id
                unless desc.object_id == server.description.object_id
                  unless desc == server.description
                    expect(desc).to be_unknown
                    expect(server.description).to be_unknown
                  end
                end
              end
            end

            if %w(ReplicaSetWithPrimary ReplicaSetNoPrimary).include?(phase.outcome.topology_type)
              it 'has expected max election id' do
                expect(@client.cluster.topology.max_election_id).to eq(phase.outcome.max_election_id)
              end

              it 'has expected max set version' do
                expect(@client.cluster.topology.max_set_version).to eq(phase.outcome.max_set_version)
              end
            end

          else

            before do
              @client.cluster.servers.each do |server|
                allow(server).to receive(:connectable?).and_return(true)
              end
            end

            it 'is incompatible' do
              expect(@client.cluster.topology.compatible?).to be false
            end

            it 'raises an UnsupportedFeatures error' do
              expect {
                p = Mongo::ServerSelector.get(mode: :primary).select_server(@client.cluster)
                s = Mongo::ServerSelector.get(mode: :secondary).select_server(@client.cluster)
                raise "UnsupportedFeatures not raised but we got #{p.inspect} as primary and #{s.inspect} as secondary"
              }.to raise_exception(Mongo::Error::UnsupportedFeatures)
            end
          end
        end
      end
    end
  end
end
