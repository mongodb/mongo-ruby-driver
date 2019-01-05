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

          let!(:results) do
            test.run(authorized_collection)
          end

          let(:verifier) { Mongo::CRUD::Verifier.new(test) }

          let(:actual_collection) do
            if test.outcome['collection'] && test.outcome['collection']['name']
              authorized_client[test.outcome['collection']['name']]
            else
              authorized_collection
            end
          end

          it 'returns the correct result' do
            verifier.verify_operation_result(results)
          end

          it 'has the correct data in the collection', if: test.outcome_collection_data do
            results
            verifier.verify_collection_data(actual_collection.find.to_a)
          end
        end
      end
    end
  end
end
