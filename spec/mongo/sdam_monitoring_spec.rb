require 'spec_helper'

describe 'SDAM Monitoring' do
  include Mongo::SDAM

  SDAM_MONITORING_TESTS.each do |file|

    spec = Mongo::SDAM::Spec.new(file)

    context(spec.description) do

      before(:all) do

        module Mongo
          # We monkey-patch the server here, so the monitors do not run and no
          # real TCP connection is attempted. Thus we can control the server
          # descriptions per-phase.
          #
          # @since 2.0.0
          class Server

            alias :original_initialize :initialize
            def initialize(address, cluster, monitoring, event_listeners, options = {})
              @address = address
              @cluster = cluster
              @monitoring = monitoring
              @options = options.freeze
              @monitor = Monitor.new(address, event_listeners, options)
            end

            alias :original_disconnect! :disconnect!
            def disconnect!; true; end
          end
        end

        # Client is set as an instance variable inside the scope of the spec to
        # retain its modifications across contexts/phases. Let is no good
        # here as we have a clean slate for each context/phase.
        @client = Mongo::Client.new(spec.uri_string)
      end

      after(:all) do
        @client.close

        # Return the server implementation to its original for the other
        # tests in the suite.
        module Mongo
          class Server
            alias :initialize :original_initialize
            remove_method(:original_initialize)

            alias :disconnect! :original_disconnect!
            remove_method(:original_disconnect!)
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
              server = Mongo::Server.new(
                Mongo::Address.new(response.address),
                @client.cluster,
                @client.instance_variable_get(:@monitoring),
                @client.cluster.send(:event_listeners),
                @client.cluster.options
              ) unless server
              monitor = server.instance_variable_get(:@monitor)
              description = monitor.inspector.run(server.description, response.ismaster, 0.5)
              monitor.instance_variable_set(:@description, description)
            end
          end

          phase.outcome.events.each do |event|

            it "expects a #{event.name} to be fired" do
            end
          end
        end
      end
    end
  end
end
