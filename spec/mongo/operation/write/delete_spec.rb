require 'spec_helper'

describe Mongo::Operation::Write::Delete do
  include_context 'operation'

  let(:document) { { :q => { :foo => 1 }, :limit => 1 } }
  let(:spec) do
    { :delete        => document,
      :db_name       => db_name,
      :coll_name     => coll_name,
      :write_concern => write_concern,
      :ordered       => true
    }
  end

  let(:delete_write_cmd) do
    double('delete_write_cmd').tap do |d|
      allow(d).to receive(:execute) { [] }
    end
  end

  let(:op) { described_class.new(spec) }

  describe '#initialize' do

    context 'spec' do

      it 'sets the spec' do
        expect(op.spec).to eq(spec)
      end
    end
  end

  describe '#==' do

    context 'spec' do

      context 'when two ops have the same specs' do
        let(:other) { described_class.new(spec) }

        it 'returns true' do
          expect(op).to eq(other)
        end
      end

      context 'when two ops have different specs' do
        let(:other_doc) { { :q => { :bar => 1 }, :limit => 1 } }
        let(:other_spec) do
          { :delete        => other_doc,
            :db_name       => db_name,
            :coll_name     => coll_name,
            :write_concern => write_concern,
            :ordered       => true
          }
        end
        let(:other) { described_class.new(other_spec) }

        it 'returns false' do
          expect(op).not_to eq(other)
        end
      end
    end
  end

  describe '#execute' do

    before do
      authorized_collection.insert_many([
        { name: 'test', field: 'test' },
        { name: 'testing', field: 'test' }
      ])
    end

    after do
      authorized_collection.delete_many
    end

    context 'when deleting a single document' do

      let(:delete) do
        described_class.new({
          delete: document,
          db_name: TEST_DB,
          coll_name: TEST_COLL,
          write_concern: Mongo::WriteConcern.get(:w => 1)
        })
      end

      context 'when the delete succeeds' do

        let(:document) do
          { 'q' => { field: 'test' }, 'limit' => 1 }
        end

        let(:result) do
          delete.execute(authorized_primary.context)
        end

        it 'deletes the documents from the database' do
          expect(result.written_count).to eq(1)
        end

        it 'reports the correct deleted count' do
          expect(result.deleted_count).to eq(1)
        end
      end

      context 'when the delete fails' do

        let(:document) do
          { que: { field: 'test' } }
        end

        it 'raises an exception' do
          expect {
            delete.execute(authorized_primary.context)
          }.to raise_error(Mongo::Error::OperationFailure)
        end
      end
    end

    context 'when deleting multiple documents' do

      let(:delete) do
        described_class.new({
          delete: document,
          db_name: TEST_DB,
          coll_name: TEST_COLL,
          write_concern: Mongo::WriteConcern.get(:w => 1)
        })
      end

      context 'when the deletes succeed' do

        let(:document) do
          { 'q' => { field: 'test' }, 'limit' => 0 }
        end

        let(:result) do
          delete.execute(authorized_primary.context)
        end

        it 'deletes the documents from the database' do
          expect(result.written_count).to eq(2)
        end

        it 'reports the correct deleted count' do
          expect(result.deleted_count).to eq(2)
        end
      end

      context 'when a delete fails' do

        let(:document) do
          failing_delete_doc
        end

        let(:result) do
          delete.execute(authorized_primary.context)
        end

        it 'does not delete any documents' do

          expect {
            op.execute(authorized_primary.context)
          }.to raise_error(Mongo::Error::OperationFailure)

          expect(authorized_collection.find.count).to eq(2)
        end
      end

      context 'when a document exceeds max bson size' do

        let(:document) do
          { 'q' => { field: 't'*17000000 }, 'limit' => 0 }
        end

        it 'raises an error' do
          expect {
            op.execute(authorized_primary.context)
          }.to raise_error(Mongo::Error::MaxBSONSize)
        end
      end
    end
  end
end
