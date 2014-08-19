require 'spec_helper'

describe Mongo::Operation::Write::DropIndex do

  describe '#execute' do

    context 'when the server is a primary' do

      context 'when the index exists' do

        let(:spec) do
          { another: -1 }
        end

        before do
          authorized_client[TEST_COLL].ensure_index(spec, unique: true)
        end

        let(:operation) do
          described_class.new(
            db_name: TEST_DB,
            coll_name: TEST_COLL,
            index_name: 'another_-1'
          )
        end

        let(:response) do
          operation.execute(authorized_primary.context)
        end

        it 'removes the index' do
          expect(response).to be_ok
        end
      end

      context 'when the index does not exist' do

        let(:operation) do
          described_class.new(
            db_name: TEST_DB,
            coll_name: TEST_COLL,
            index_name: 'another_blah'
          )
        end

        it 'raises an exception' do
          expect {
            operation.execute(authorized_primary.context)
          }.to raise_error(Mongo::Operation::Write::Failure)
        end
      end
    end

    context 'when the server is a secondary' do

      pending 'raises an exception'
    end
  end
end
