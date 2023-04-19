# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Operation::Delete do
  require_no_required_api_version

  before do
    begin
      authorized_collection.delete_many
    rescue Mongo::Error::OperationFailure
    end
    begin
      authorized_collection.indexes.drop_all
    rescue Mongo::Error::OperationFailure
    end
  end

  let(:document) do
    {
      :q => { :foo => 1 },
      :limit => 1
    }
  end

  let(:spec) do
    { :deletes        => [ document ],
      :db_name       => SpecConfig.instance.test_db,
      :coll_name     => TEST_COLL,
      :write_concern => Mongo::WriteConcern.get(w: :majority),
      :ordered       => true
    }
  end

  let(:op) { described_class.new(spec) }

  let(:context) { Mongo::Operation::Context.new }

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
          { :deletes        => [ other_doc ],
            :db_name       => SpecConfig.instance.test_db,
            :coll_name     => TEST_COLL,
            :write_concern => Mongo::WriteConcern.get(w: :majority),
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
          deletes: [ document ],
          db_name: SpecConfig.instance.test_db,
          coll_name: TEST_COLL,
          write_concern: Mongo::WriteConcern.get(w: :majority)
        })
      end

      context 'when the delete succeeds' do

        let(:document) do
          { 'q' => { field: 'test' }, 'limit' => 1 }
        end

        let(:result) do
          delete.execute(authorized_primary, context: context)
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
            delete.execute(authorized_primary, context: context)
          }.to raise_error(Mongo::Error::OperationFailure)
        end
      end
    end

    context 'when deleting multiple documents' do

      let(:delete) do
        described_class.new({
          deletes: [ document ],
          db_name: SpecConfig.instance.test_db,
          coll_name: TEST_COLL,
          write_concern: Mongo::WriteConcern.get(w: :majority)
        })
      end

      context 'when the deletes succeed' do

        let(:document) do
          { 'q' => { field: 'test' }, 'limit' => 0 }
        end

        let(:result) do
          delete.execute(authorized_primary, context: context)
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
          { q: { '$set' => { a: 1 } }, limit: 0 }
        end

        let(:result) do
          delete.execute(authorized_primary, context: context)
        end

        it 'does not delete any documents' do

          expect {
            op.execute(authorized_primary, context: context)
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
            op.execute(authorized_primary, context: context)
          }.to raise_error(Mongo::Error::MaxBSONSize)
        end
      end
    end
  end
end
