require 'spec_helper'

describe Mongo::Operation::Read::CollectionNames do

  let(:spec) do
    { :db_name => TEST_DB }
  end

  let(:collection_names) do
    [ 'berlin', 'london' ]
  end

  let(:op) do
    described_class.new(spec)
  end

  describe '#execute' do

    context '#names is called on the result' do

      before do
        collection_names.each do |name|
          authorized_client[name].insert_one(x: 1)
        end
      end

      after do
        collection_names.each do |name|
          authorized_client[name].drop
        end
      end

      let(:names) do
        op.execute(authorized_primary.context).names
      end

      it 'returns the list of collection names' do
        expect(names).to include(*collection_names)
      end
    end
  end
end
