require 'spec_helper'

require 'runners/crud'
require 'runners/change_streams/spec'

describe 'ChangeStreams' do
  require_wired_tiger
  require_no_multi_shard

  CHANGE_STREAMS_TESTS.each do |file|

    spec = Mongo::ChangeStreams::Spec.new(file)

    context(spec.description) do

      spec.tests.each do |test|

        context(test.description) do
          require_topology *test.topologies

          before do
            unless test.server_version_satisfied?(authorized_client)
              skip 'Version requirements not satisfied'
            end

            test.setup_test
          end

          after do
            test.teardown_test
          end

          let(:result) do
            test.run
          end

          let(:verifier) { Mongo::CRUD::Verifier.new(test) }

          if test.outcome.error?
            let(:actual_error) { result[:result][:error] }

            it 'fails' do
              actual_error[:code].should == test.outcome.error.fetch('code')
            end

            if test.outcome.error['errorLabels']
              it 'has correct error labels' do
                actual_error[:labels].should == test.outcome.error.fetch('errorLabels')
              end
            end
          else
            let(:actual_documents) do
              result[:result]['success'].map do |document|
                document = document.dup
                fd = document['fullDocument']
                if fd
                  fd = fd.dup
                  fd.delete('_id')
                  document['fullDocument'] = fd
                end
                document
              end
            end

            it 'has the correct documents' do
              verifier.verify_operation_result(test.result['success'], actual_documents)
            end
          end

          if test.expectations
            let(:actual_events) do
              result[:events]
            end

            it 'has the correct number of command_started events' do
              verifier.verify_command_started_event_min_count(test.expectations, actual_events)
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
