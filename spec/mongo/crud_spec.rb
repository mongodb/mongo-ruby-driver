require 'spec_helper'

describe 'Server Discovery and Monitoring' do
  include Mongo::SDAM

  CRUD_TESTS.each do |file|

    spec = Mongo::CRUD::Spec.new(file)

    context(spec.description) do

      spec.tests.each do |test|

        context(test.description) do

          before(:each) do
            authorized_collection.find.delete_many
          end

          after(:each) do
            authorized_collection.find.delete_many
          end

          it "returns the correct result" do
            expect(test.run(authorized_collection)).to eq(test.result)
          end
        end
      end
    end
  end
end
