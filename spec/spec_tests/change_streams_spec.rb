require 'spec_helper'

describe 'ChangeStreams' do
  require_wired_tiger

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

          let(:verifier) { Mongo::CRUD::Verifier.new(test) }

          it 'returns the correct result' do
            expect(result[:result]).to match_result(test)
          end

          if test.expectations
            let(:actual_events) do
              result[:events]
            end

            it 'has the correct number of command_started events' do
              verifier.verify_command_started_event_count(test.expectations, actual_events)
            end

            test.expectations.each_with_index do |expectation, i|
              it "has the correct command_started event #{i+1}" do
                verifier.verify_command_started_event(
                  test.expectations, actual_events, i)
              end
            end
          end
        end
      end
    end
  end
end
