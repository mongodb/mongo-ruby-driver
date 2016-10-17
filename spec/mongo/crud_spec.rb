require 'spec_helper'

describe 'CRUD' do

  CRUD_TESTS.each do |file|

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

          let(:results) do
            test.run(authorized_collection)
          end

          it 'returns the correct result' do
            skip 'Test cannot be run on this server version' unless spec.server_version_satisfied?(authorized_client)
            expect(results).to match_operation_result(test)
          end

          it 'has the correct data in the collection', if: test.outcome_collection_data do
            skip 'Test cannot be run on this server version' unless spec.server_version_satisfied?(authorized_client)
            results
            expect(authorized_collection.find.to_a).to match_collection_data(test)
          end
        end
      end
    end
  end
end
