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
              new_description = Mongo::Server::Description.new(
                server.description.address, response.ismaster, 0.5)
              publisher = SdamSpecEventPublisher.new(@client.cluster.send(:event_listeners))
              publisher.publish(Mongo::Event::DESCRIPTION_CHANGED, server.description, new_description)
              monitor.instance_variable_set(:@description, new_description)
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

            phase.outcome.servers.each do |uri, server_spec|

              it "sets #{uri} to #{server_spec['type']}" do
                server = find_server(@client, uri)
                unless server_of_type?(server, server_spec['type'])
                  raise RSpec::Expectations::ExpectationNotMetError,
                    "Server #{server.summary} not of type #{server_spec['type']}"
                end
              end

              it "sets #{uri} replica set name to #{server_spec['setName'].inspect}" do
                expect(find_server(@client, uri).replica_set_name).to eq(server_spec['setName'])
              end
            end

          else

            before do
              @client.cluster.servers.each do |server|
                allow(server).to receive(:connectable?).and_return(true)
              end
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
