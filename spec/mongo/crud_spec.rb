require 'spec_helper'

describe 'CRUD' do

  CRUD_TESTS.each do |file|

    spec = Mongo::CRUD::Spec.new(file)

    context(spec.description) do

      spec.tests.each do |test|

        context(test.description) do

          before(:each) do
            authorized_collection.find.delete_many
          end

          let(:results) do
            test.run(authorized_collection)
          end

          after(:each) do
            authorized_collection.find.delete_many
          end

          it "returns the correct result" do
            expect(results).to match_results(test)
          end

          it 'has the correct data in the collection' do
            expect(test.run(authorized_collection)).to match_collection_data(test)
          end
        end
      end
    end
  end
end
