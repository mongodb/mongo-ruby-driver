require 'spec_helper'

describe Mongo::Operation::Write::CreateIndex do

  describe '#execute' do

    context 'when the index is created' do

      let(:spec) do
        { key: { random: 1 }, name: 'random_1', unique: true }
      end

      let(:operation) do
        described_class.new(indexes: [ spec ], db_name: TEST_DB, coll_name: TEST_COLL)
      end

      let(:response) do
        operation.execute(authorized_primary.context)
      end

      after do
        authorized_collection.indexes.drop_one('random_1')
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
        described_class.new(indexes: [ spec ], db_name: TEST_DB, coll_name: TEST_COLL)
      end

      let(:second_operation) do
        described_class.new(indexes: [ spec.merge(unique: false) ], db_name: TEST_DB, coll_name: TEST_COLL)
      end

      before do
        operation.execute(authorized_primary.context)
      end

      after do
        authorized_collection.indexes.drop_one('random_1')
      end

      it 'raises an exception', if: write_command_enabled? do
        expect {
          second_operation.execute(authorized_primary.context)
        }.to raise_error(Mongo::Error::OperationFailure)
      end

      it 'does not raise an exception', unless: write_command_enabled? do
        expect(second_operation.execute(authorized_primary.context)).to be_successful
      end
    end
  end
end
