require 'spec_helper'

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
