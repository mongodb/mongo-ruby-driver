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

  SERVER_DISCOVERY_TESTS.each do |file|

    test = YAML.load(ERB.new(File.new(file).read).result)

    # Description will be mapped to the name of the spec.
    description = test['description']

    # Phases are for each step in the spec.
    phases = test['phases']

    # URI in which the Mongo::Client will be instantiated with.
    uri = test['uri']

    context(description) do

      let(:client) do
        Mongo::Client.new(uri)
      end

      phases.each do |phase|

        # The responses for each server ismaster call during the phase.
        responses = phase['responses']

        # The outcome (expected cluster topology) for each phase.
        outcome = phase['outcome']

        responses.each do |map|

          # The response map for each ismaster call for each server in the
          # phase.
          response = Hash[*map]
        end

        it "sets the cluster topology to #{outcome['topologyType']}" do

        end

        it "sets the cluster replica set name to #{outcome['setName'].inspect}" do

        end

        outcome['servers'].each do |uri, server|

          it "sets #{uri} to #{server['type']}" do

          end

          it "sets #{uri} replica set name to #{server['setName'].inspect}" do

          end
        end
      end
    end
  end
end
