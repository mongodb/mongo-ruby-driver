require 'spec_helper'

describe 'Server Discovery and Monitoring' do
  include Mongo::SDAM

  SERVER_DISCOVERY_TESTS.each do |file|

    spec = Mongo::SDAM::Spec.new(file)

    context(spec.description) do

      before(:all) do
        @client = Mongo::Client.new([])
        @client.send(:create_from_uri, spec.uri_string)
        client_options = @client.instance_variable_get(:@options)
        @client.instance_variable_set(:@options, client_options.merge(heartbeat_frequency: 100, connect_timeout: 0.1))
        @client.cluster.instance_variable_set(:@options, client_options.merge(heartbeat_frequency: 100, connect_timeout: 0.1))
        @client.cluster.instance_variable_get(:@servers).each { |s| s.disconnect!; s.unknown!; }
      end

      after(:all) do
        @client.close
      end

      spec.phases.each_with_index do |phase, index|

        context("Phase: #{index + 1}") do

          before(:all) do
            phase.responses.each do |response|
              server = find_server(@client, response.address)
              unless server
                server = Mongo::Server.new(
                    Mongo::Address.new(response.address),
                    @client.cluster,
                    @client.instance_variable_get(:@monitoring),
                    @client.cluster.send(:event_listeners),
                    @client.cluster.options
                )
                server.disconnect!
                server.unknown!
              end
              monitor = server.instance_variable_get(:@monitor)
              description = monitor.inspector.run(server.description, response.ismaster, 0.5)
              monitor.instance_variable_set(:@description, description)
            end
          end

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

          it "has the expected servers in the cluster" do
            expect(cluster_addresses).to eq(phase_addresses)
          end

          phase.outcome.servers.each do |uri, server|

            it "sets #{uri} to #{server['type']}" do
              expect(find_server(@client, uri)).to be_server_type(server['type'])
            end

            it "sets #{uri} replica set name to #{server['setName'].inspect}" do
              expect(find_server(@client, uri).replica_set_name).to eq(server['setName'])
            end
          end
        end
      end
    end
  end
end
