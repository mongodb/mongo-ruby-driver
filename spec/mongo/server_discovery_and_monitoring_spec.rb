require 'spec_helper'

describe 'Server Discovery and Monitoring' do
  include Mongo::SDAM

  SERVER_DISCOVERY_TESTS.each do |file|

    spec = Mongo::SDAM::Spec.new(file)

    context(spec.description) do

      before(:all) do

        # We monkey-patch the server here, so the monitors do not run and no
        # real TCP connection is attempted. Thus we can control the server
        # descriptions per-phase.
        #
        # @since 2.0.0
        class Mongo::Server

          # Provides the ability to get and set the description from outside the class.
          attr_accessor :description

          # Provide a reader for event listeners to pass them to new
          # descriptions.
          attr_reader :event_listeners

          # The contructor keeps the same API, but does not instantiate a
          # monitor and run it.
          def initialize(address, event_listeners, options = {})
            @event_listeners = event_listeners
            @address = address
            @options = options.freeze
            @description = Description.new(@address, {}, event_listeners)
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

        # Return the server implementation to its original form the the other
        # tests in the suite.
        class Mongo::Server

          # Returns the constructor to its original implementation.
          def initialize(address, event_listeners, options = {})
            @address = address
            @options = options.freeze
            @description = Description.new(address, {}, event_listeners)
            @monitor = Monitor.new(address, description, options)
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
      end

      spec.phases.each_with_index do |phase, index|

        context("Phase: #{index + 1}") do

          phase.responses.each do |response|

            before do
              # For each response in the phase, we need to change that server's
              # description.
              server = find_server(@client, response.address)
              server.description.update!(response.ismaster, 0.5)
            end
          end

          it "sets the cluster topology to #{phase.outcome.topology_type}" do
            expect(@client.cluster).to be_topology(phase.outcome.topology_type)
          end

          it "sets the cluster replica set name to #{phase.outcome.set_name.inspect}" do
            expect(@client.cluster.replica_set_name).to eq(phase.outcome.set_name)
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
