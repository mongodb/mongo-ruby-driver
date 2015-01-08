require 'spec_helper'

# Matcher for determining if the server is of the expected type according to
# the test.
RSpec::Matchers.define :be_server_type do |expected|

  match do |actual|
    case expected
      when 'Standalone' then actual.standalone?
      when 'RSPrimary' then actual.primary?
      when 'RSSecondary' then actual.secondary?
      when 'RSArbiter' then actual.arbiter?
      when 'Mongos' then actual.mongos?
      when 'Unknown' then actual.unknown?
      when 'PossiblePrimary' then actual.unknown?
      when 'RSGhost' then actual.ghost?
    end
  end
end

# Matcher for determining if the cluster topology is the expected type.
RSpec::Matchers.define :be_topology do |expected|

  match do |actual|
    case expected
      when 'ReplicaSetWithPrimary' then actual.replica_set?
      when 'ReplicaSetNoPrimary' then actual.replica_set?
      when 'Sharded' then actual.sharded?
      when 'Single' then actual.standalone?
      when 'Unknown' then actual.standalone?
    end
  end
end

def find_server(client, uri)
  client.cluster.instance_variable_get(:@servers).detect{ |s| s.address.to_s == uri }
end

describe 'Server Discovery and Monitoring' do

  SERVER_DISCOVERY_TESTS.take(8).each do |file|

    spec = Mongo::SDAM::Spec.new(file)

    context(spec.description) do

      before(:all) do
        class Mongo::Server
          attr_accessor :description
          attr_reader :event_listeners
          def initialize(address, event_listeners, options = {})
            @event_listeners = event_listeners
            @address = Address.new(address)
            @options = options.freeze
            @description = Description.new({}, event_listeners)
          end
          def disconnect!; true; end
        end

        @client = Mongo::Client.new(spec.uri_string)
      end

      after(:all) do
        class Mongo::Server
          def initialize(address, event_listeners, options = {})
            @address = Address.new(address)
            @options = options.freeze
            @monitor = Monitor.new(self, options)
            @description = Description.new({}, event_listeners)
            @monitor.check!
            @monitor.run
          end
          def disconnect!
            context.with_connection do |connection|
              connection.disconnect!
            end
            @monitor.stop and true
          end
        end
      end

      spec.phases.each_with_index do |phase, index|

        context("Phase: #{index + 1}") do

          phase.responses.each do |response|

            before do
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
              srv = find_server(@client, uri)
              expect(srv).to be_server_type(server['type'])
            end

            it "sets #{uri} replica set name to #{server['setName'].inspect}" do
              srv = find_server(@client, uri)
              expect(srv.replica_set_name).to eq(server['setName'])
            end
          end
        end
      end
    end
  end
end
