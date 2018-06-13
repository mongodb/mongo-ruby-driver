require 'spec_helper'

describe 'Retryable writes spec tests' do

  RETRYABLE_WRITES_TESTS.each do |file|

    spec = Mongo::CRUD::Spec.new(file)

    context(spec.description) do

      spec.tests.each do |test|

        context(test.description) do

          let(:collection) do
            client[TEST_COLL]
          end

          let(:client) do
            authorized_client_with_retry_writes
          end

          before do
            unless sessions_enabled?
              skip 'Sessions not enabled'
            end
            unless replica_set?
              skip 'Not in replica set'
            end
            unless spec.server_version_satisfied?(client)
              skip 'Test cannot be run on this server version'
            end
          end

          before do
            test.setup_test(collection)
          end

          after do
            test.clear_fail_point(collection)
            collection.delete_many
          end

          let(:results) do
            if test.error?
              error = nil
              begin
                test.run(collection)
              rescue => e
                error = e
              end
              error
            else
              test.run(collection)
            end
          end

          if test.outcome_collection_data
            it 'has the correct data in the collection' do
              results
              expect(collection.find.to_a).to match_collection_data(test)
            end
          end

          if test.error?
            it 'raises an error' do
              expect(results).to be_a(Mongo::Error)
            end
          else
            it 'returns the correct result' do
              expect(results).to match_operation_result(test)
            end
          end
        end
      end
    end
  end
end
