require 'spec_helper'

describe 'CRUD' do

  CRUD_TESTS.each do |file|

    spec = Mongo::CRUD::Spec.new(file)

    context(spec.description) do

      spec.tests.each do |test|

        around do |example|
          if spec.server_version_satisfied?(authorized_client)
            example.run
          end
        end

        context(test.description) do

          before(:each) do
            test.setup_test(authorized_collection)
          end

          after(:each) do
            authorized_collection.delete_many
          end

          let(:results) do
            test.run(authorized_collection)
          end

          it 'returns the correct result' do
            expect(results).to match_operation_result(test)
          end

          it 'has the correct data in the collection', if: test.outcome_collection_data do
            results
            expect(authorized_collection.find.to_a).to match_collection_data(test)
          end
        end
      end
    end
  end
end
