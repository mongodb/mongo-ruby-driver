require 'spec_helper'

describe 'CRUD' do

  test_files = CRUD_TESTS
  test_files += CRUD_TESTS_3_4 if collation_enabled?
  test_files += CRUD_TESTS_2_6 if write_command_enabled?

  test_files.each do |file|

    spec = Mongo::CRUD::Spec.new(file)

    context(spec.description) do

      spec.tests.each do |test|

        context(test.description) do

          before(:each) do
            authorized_collection.delete_many
          end

          after(:each) do
            authorized_collection.delete_many
          end

          let!(:results) do
            test.run(authorized_collection)
          end

          it 'returns the correct result' do
            expect(results).to match_operation_result(test)
          end

          it 'has the correct data in the collection', if: test.outcome_collection_data do
            expect(authorized_collection.find.to_a).to match_collection_data(test)
          end
        end
      end
    end
  end
end
