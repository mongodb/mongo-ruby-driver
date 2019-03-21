require 'spec_helper'

describe 'CRUD' do

  CRUD_TESTS.each do |file|

    spec = Mongo::CRUD::Spec.new(file)

    context(spec.description) do

      spec.tests.each do |test|

        context(test.description) do

          before(:each) do
            unless spec.server_version_satisfied?(authorized_client)
              skip 'Version requirement not satisfied'
            end

            test.setup_test(authorized_collection)
          end

          after(:each) do
            authorized_collection.delete_many
          end

          let(:verifier) { Mongo::CRUD::Verifier.new(test) }

          test.operations.each_with_index do |operation, index|
            context "operation #{index+1}" do

              let!(:result) do
                test.run(authorized_collection, index+1)
              end

              let(:actual_collection) do
                if operation.outcome && operation.outcome.collection_name
                  authorized_client[operation.outcome.collection_name]
                else
                  authorized_collection
                end
              end

              it 'returns the correct result' do
                verifier.verify_operation_result(operation, result)
              end

              it 'has the correct data in the collection', if: operation.outcome.collection_data? do
                verifier.verify_collection_data(operation, actual_collection.find.to_a)
              end
            end
          end
        end
      end
    end
  end
end
