require 'spec_helper'

describe 'Retryable writes spec tests' do

  RETRYABLE_WRITES_TESTS.each do |file|

    spec = Mongo::CRUD::Spec.new(file)

    context(spec.description) do

      spec.tests.each do |test|

        context(test.description) do
          # Retryable writes work on 3.6 servers but fail points only
          # exist in 4.0 and higher
          min_server_fcv '4.0'
          require_topology :replica_set

          let(:collection) do
            client[TEST_COLL]
          end

          let(:client) do
            authorized_client_with_retry_writes
          end

          before do
            unless spec.server_version_satisfied?(client)
              skip 'Test cannot be run on this server version'
            end
          end

          before do
            if spec.server_version_satisfied?(client)
              test.setup_test(collection)
            end
          end

          after do
            if spec.server_version_satisfied?(client)
              test.clear_fail_point(collection.client)
              collection.delete_many
            end
          end

          let(:verifier) { Mongo::CRUD::Verifier.new(test) }

          test.operations.each_with_index do |operation, index|
            context "operation #{index+1}" do

              let(:result) do
                if operation.outcome.error?
                  error = nil
                  begin
                    test.run(collection, index+1)
                  rescue => e
                    error = e
                  end
                  error
                else
                  test.run(collection, index+1)
                end
              end

              it 'has the correct data in the collection', if: operation.outcome.collection_data? do
                result
                verifier.verify_collection_data(
                  operation.outcome.collection_data,
                  authorized_collection.find.to_a)
              end

              if operation.outcome.error?
                it 'raises an error' do
                  expect(result).to be_a(Mongo::Error)
                end
              else
                it 'returns the correct result' do
                  verifier.verify_operation_result(operation.outcome.result, result)
                end
              end
            end
          end
        end
      end
    end
  end
end
