require 'spec_helper'

describe 'Server Discovery and Monitoring' do
  include Mongo::SDAM

  SERVER_DISCOVERY_TESTS.each do |file|

    spec = Mongo::SDAM::Spec.new(file)

    context(spec.description) do

      before(:all) do

        # We monkey-patch the address, so that looking up the spec's hostname does
        # not throw an error.
        #
        # @since 2.0.0
        class Mongo::Address
          private

          def family(host)
            fam = host == 'localhost' ? ::Socket::AF_INET : ::Socket::AF_UNSPEC
            ::Socket.getaddrinfo(host, nil, fam, ::Socket::SOCK_STREAM).first[4]
          rescue SocketError
          end
        end

        # We monkey-patch the server here, so the monitors do not run and no
        # real TCP connection is attempted. Thus we can control the server
        # descriptions per-phase.
        #
        # @since 2.0.0
        class Mongo::Server

          # The constructor keeps the same API, but does not instantiate a
          # monitor and run it.
          def initialize(address, cluster, event_listeners, options = {})
            @address = address
            @cluster = cluster
            @options = options.freeze
            @monitor = Monitor.new(address, event_listeners, options)
          end

          # Disconnect simply needs to return true since we have no monitor and
          # no connection.
          def disconnect!; true; end
        end

        # Client is set as an instance variable inside the scope of the spec to
        # retain its modifications across contexts/phases. Let is no good
        # here as we have a clean slate for each context/phase.
        @client = Mongo::Client.new(spec.uri_string)
      end

      after(:all) do

        # Return the server implementation to its original for the other
        # tests in the suite.
        class Mongo::Server

          # Returns the constructor to its original implementation.
          def initialize(address, cluster, event_listeners, options = {})
            @address = address
            @cluster = cluster
            @options = options.freeze
            @monitor = Monitor.new(address, event_listeners, options)
            @monitor.scan!
            @monitor.run!
          end

          # Returns disconnect! to its original implementation.
          def disconnect!
            context.with_connection do |connection|
              connection.disconnect!
            end
            @monitor.stop! and true
          end
        end

        class Mongo::Address
          private

          def family(host)
            fam = host == 'localhost' ? ::Socket::AF_INET : ::Socket::AF_UNSPEC
            ::Socket.getaddrinfo(host, nil, fam, ::Socket::SOCK_STREAM).first[4]
          end
        end
      end

      spec.phases.each_with_index do |phase, index|

        context("Phase: #{index + 1}") do

          phase.responses.each do |response|
            before do
              # For each response in the phase, we need to change that server's
              # description.
              server = find_server(@client, response.address)
              server = Mongo::Server.new(Mongo::Address.new(response.address), @client.cluster,
                                         @client.cluster.send(:event_listeners), @client.cluster.options) unless server
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
