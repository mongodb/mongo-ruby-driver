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

          it "returns the correct result" do
            skip 'Test results only match with server version >= 2.6' if test.requires_2_6?(write_command_enabled?,
                                                                                            authorized_collection)
            expect(results).to eq(test.result)
          end

          it 'has the correct data in the collection' do
            skip 'Test results only match with server version >= 2.6' if test.requires_2_6?(write_command_enabled?,
                                                                                            authorized_collection)
            expect(test.run(authorized_collection)).to match_collection_data(test)
          end
        end
      end
    end
  end
end
