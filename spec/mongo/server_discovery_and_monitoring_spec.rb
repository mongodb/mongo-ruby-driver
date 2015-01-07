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
      when 'Unknown' then actual.unknown?
    end
  end
end

def find_server(cluster, uri)
  cluster.instance_variable_get(:@servers).detect{ |s| s.address.to_s == uri }
end

describe 'Server Discovery and Monitoring' do

  SERVER_DISCOVERY_TESTS.take(2).each do |file|

    spec = Mongo::SDAM::Spec.new(file)

    context(spec.description) do

      let(:client) do
        Mongo::Client.new(spec.uri_string)
      end

      spec.phases.each do |phase|

        phase.responses.each do |response|

          before do
            # Provide the expectation on the connection for the ismaster
            # command and return the reply.
            expect(Mongo::Server).to receive(:new).
              with(
                response.address,
                instance_of(Mongo::Event::Listeners),
                spec.uri.client_options
              ) do |addr, listeners, options|
                Mongo::SDAM::Server.new(addr, listeners, options, response.ismaster)
              end

            allow(Mongo::Server).to receive(:new).
              with(
                instance_of(String),
                instance_of(Mongo::Event::Listeners),
                spec.uri.client_options
              ) do |addr, listeners, options|
                Mongo::SDAM::Server.new(addr, listeners, options, {})
              end
          end
        end

        it "sets the cluster topology to #{phase.outcome.topology_type}" do
          expect(client.cluster).to be_topology(phase.outcome.topology_type)
        end

        it "sets the cluster replica set name to #{phase.outcome.set_name}" do
          expect(client.cluster.replica_set_name).to eq(phase.outcome.set_name)
        end

        phase.outcome.servers.each do |uri, server|

          it "sets #{uri} to #{server['type']}" do
            srv = find_server(client.cluster, uri)
            expect(srv).to be_server_type(server['type'])
          end

          it "sets #{uri} replica set name to #{server['setName'].inspect}" do
            srv = find_server(client.cluster, uri)
            expect(srv.replica_set_name).to eq(server['setName'])
          end
        end
      end
    end
  end
end
