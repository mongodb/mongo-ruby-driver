require 'spec_helper'

describe Mongo::Operation::CreateIndex do

  before do
    authorized_collection.drop
    authorized_collection.insert_one(test: 1)
  end

  describe '#execute' do

    context 'when the index is created' do

      let(:spec) do
        { key: { random: 1 }, name: 'random_1', unique: true }
      end

      let(:operation) do
        described_class.new(indexes: [ spec ], db_name: SpecConfig.instance.test_db, coll_name: TEST_COLL)
      end

      let(:response) do
        operation.execute(authorized_primary)
      end

      it 'returns ok' do
        expect(response).to be_successful
      end
    end

    context 'when index creation fails' do

      let(:spec) do
        { key: { random: 1 }, name: 'random_1', unique: true }
      end

      let(:operation) do
        described_class.new(indexes: [ spec ], db_name: SpecConfig.instance.test_db, coll_name: TEST_COLL)
      end

      let(:second_operation) do
        described_class.new(indexes: [ spec.merge(unique: false) ], db_name: SpecConfig.instance.test_db, coll_name: TEST_COLL)
      end

      before do
        operation.execute(authorized_primary)
      end

      it 'raises an exception' do
        expect {
          second_operation.execute(authorized_primary)
        }.to raise_error(Mongo::Error::OperationFailure)
      end
    end
  end
end
