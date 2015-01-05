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
      when 'Unknown' then actual.ghost?
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

describe 'Server Discovery and Monitoring' do

  SERVER_DISCOVERY_TESTS.take(1).each do |file|

    spec = Mongo::SDAM::Spec.new(file)

    context(spec.description) do

      let(:client) do
        Mongo::Client.new(spec.uri_string)
      end

      spec.phases.each do |phase|

        # The responses for each server ismaster call during the phase.
        p "###################### RESPONSES FOR PHASE ######################"
        p phase.responses

        # The outcome (expected cluster topology) for each phase.
        p "###################### OUTCOME FOR PHASE ######################"
        p phase.outcome

        p "###################### CONNECTION MOCKS #######################"
        servers = phase.outcome.servers

        let(:connections) do
          servers.keys.reduce({}) do |mocks, address|
            mock = double(address)
            expect(Mongo::Connection).to receive(:new).
              with(Mongo::Server.new(address, {})).
              and_return(mock)
            mocks[address] = mock
            mocks
          end
        end

        phase.responses.each do |response|

          # The response map for each ismaster call for each server in the
          # phase.
          p "###################### RESPONSE FOR EACH SERVER ######################"
          p response

          before do
            # Provide the expectation on the connection for the ismaster
            # command and return the reply.
            expect(connections[response.address]).to receive(:dispatch).
              with([ Mongo::Server::Monitor::ISMASTER ]).
              and_return(response.reply)
          end
        end

        it "sets the cluster topology to #{phase.outcome.topology_type}" do
          p client
        end

        it "sets the cluster replica set name to #{phase.outcome.set_name}" do

        end

        phase.outcome.servers.each do |uri, server|

          it "sets #{uri} to #{server['type']}" do

          end

          it "sets #{uri} replica set name to #{server['setName'].inspect}" do

          end
        end
      end
    end
  end
end
