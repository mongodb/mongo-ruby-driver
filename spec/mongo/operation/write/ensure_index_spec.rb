require 'spec_helper'

describe Mongo::Operation::Write::EnsureIndex do

  describe '#execute' do

    context 'when the server is primary' do

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
            opts: { unique: true }
          )
        end

        let(:response) do
          operation.execute(authorized_primary.context)
        end

        after do
          authorized_client[TEST_COLL].drop_index(spec)
        end

        it 'returns ok' do
          expect(response).to be_ok
        end
      end

      context 'when index creation fails' do

        let(:spec) do
          { name: 1 }
        end

        let(:operation) do
          described_class.new(
            index: spec,
            db_name: TEST_DB,
            coll_name: TEST_COLL,
            index_name: 'random_1',
            opts: { unique: true }
          )
        end

        let(:second_operation) do
          described_class.new(
            index: spec,
            db_name: TEST_DB,
            coll_name: TEST_COLL,
            index_name: 'random_1',
            opts: { unique: false }
          )
        end

        before do
          operation.execute(authorized_primary.context)
        end

        after do
          authorized_client[TEST_COLL].drop_index(spec)
        end

        it 'raises an exception', if: write_command_enabled? do
          expect {
            second_operation.execute(authorized_primary.context)
          }.to raise_error(Mongo::Operation::Write::Failure)
        end

        it 'does not raise an exception', unless: write_command_enabled? do
          expect(second_operation.execute(authorized_primary.context)).to be_ok
        end
      end
    end

    context 'when the server is secondary' do

      pending 'it raises an exception'
    end
  end
end
