require 'spec_helper'

describe Mongo::Operation::Write::CreateIndex do

  describe '#execute' do

    context 'when the index is created' do

      let(:spec) do
        { random: 1 }
      end

      let(:operation) do
        described_class.new(
          index: spec,
          db_name: TEST_DB,
          coll_name: TEST_COLL,
          index_name: 'random_1',
          options: { unique: true }
        )
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
        { random: 1 }
      end

      let(:operation) do
        described_class.new(
          index: spec,
          db_name: TEST_DB,
          coll_name: TEST_COLL,
          index_name: 'random_1',
          options: { unique: true }
        )
      end

      let(:second_operation) do
        described_class.new(
          index: spec,
          db_name: TEST_DB,
          coll_name: TEST_COLL,
          index_name: 'random_1',
          options: { unique: false }
        )
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
