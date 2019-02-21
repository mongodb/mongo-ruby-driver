require 'spec_helper'

describe 'ChangeStreams' do

  CHANGE_STREAMS_TESTS.each do |file|

    spec = Mongo::ChangeStreams::Spec.new(file)

    context(spec.description) do

      spec.tests.each do |test|

        context(test.description) do
          require_topology *test.topologies

          before(:each) do
            unless test.server_version_satisfied?(authorized_client)
              skip 'Version requirements not satisfied'
            end

            test.setup_test
          end

          let(:result) do
            test.run
          end

          it 'returns the correct result' do
            expect(result[:result]).to match_result(test)
          end

          it 'has the correct command_started events' do
            expect(result[:events]).to match_commands(test)
          end
        end
      end
    end
  end
end
