require 'spec_helper'

describe 'CRUD' do

  test_files = if collation_enabled?
      CRUD_TESTS_3_4
    elsif find_command_enabled?
      CRUD_TESTS_3_2
    elsif list_command_enabled?
      CRUD_TESTS_3_0
    elsif write_command_enabled?
      CRUD_TESTS_2_6
    else
      CRUD_TESTS_2_4
    end

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
