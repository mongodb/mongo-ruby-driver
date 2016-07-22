require 'spec_helper'

describe Mongo::Operation::Write::Bulk::Insert do

  let(:documents) do
    [{ :name => 'test' }]
  end

  let(:write_concern) do
    Mongo::WriteConcern.get(WRITE_CONCERN)
  end

  let(:spec) do
    { documents: documents,
      db_name: authorized_collection.database.name,
      coll_name: authorized_collection.name,
      write_concern: write_concern
    }
  end

  let(:op) do
    described_class.new(spec)
  end

  after do
    authorized_collection.delete_many
  end

  describe '#initialize' do

    context 'spec' do

      it 'sets the spec' do
        expect(op.spec).to eq(spec)
      end
    end
  end

  describe '#==' do

    context 'spec' do

      context 'when two inserts have the same specs' do

        let(:other) do
          described_class.new(spec)
        end

        it 'returns true' do
          expect(op).to eq(other)
        end
      end

      context 'when two inserts have different specs' do

        let(:other_docs) do
          [{ :bar => 1 }]
        end

        let(:other_spec) do
          { :documents     => other_docs,
            :db_name       => 'test',
            :coll_name     => 'coll_name',
            :write_concern => { 'w' => 1 },
            :ordered       => true
          }
        end

        let(:other) do
          described_class.new(other_spec)
        end

        it 'returns false' do
          expect(op).not_to eq(other)
        end
      end
    end
  end

  describe 'document ids' do

    context 'when documents do not contain an id' do

      let(:documents) do
        [{ 'field' => 'test' },
         { 'field' => 'test' }]
      end

      let(:inserted_ids) do
        op.execute(authorized_primary).inserted_ids
      end

      let(:collection_ids) do
        authorized_collection.find(field: 'test').collect { |d| d['_id'] }
      end

      it 'adds an id to the documents' do
        expect(inserted_ids).to eq(collection_ids)
      end
    end
  end

  describe '#execute' do

    before do
      authorized_collection.indexes.create_one({ name: 1 }, { unique: true })
    end

    after do
      authorized_collection.delete_many
      authorized_collection.indexes.drop_one('name_1')
    end

    context 'when inserting a single document' do

      context 'when the insert succeeds' do

        let(:response) do
          op.execute(authorized_primary)
        end

        it 'inserts the documents into the database', if: write_command_enabled? do
          expect(response.written_count).to eq(1)
        end

        it 'inserts the documents into the database', unless: write_command_enabled? do
          expect(response.written_count).to eq(0)
        end
      end
    end

    context 'when inserting multiple documents' do

      context 'when the insert succeeds' do

        let(:documents) do
          [{ name: 'test1' }, { name: 'test2' }]
        end

        let(:response) do
          op.execute(authorized_primary)
        end

        it 'inserts the documents into the database', if: write_command_enabled? do
          expect(response.written_count).to eq(2)
        end

        it 'inserts the documents into the database', unless: write_command_enabled? do
          expect(response.written_count).to eq(0)
        end
      end
    end

    context 'when the inserts are ordered' do

      let(:documents) do
        [{ name: 'test' }, { name: 'test' }, { name: 'test1' }]
      end

      let(:spec) do
        { documents: documents,
          db_name: authorized_collection.database.name,
          coll_name: authorized_collection.name,
          write_concern: write_concern,
          ordered: true
        }
      end

      let(:failing_insert) do
        described_class.new(spec)
      end

      context 'when write concern is acknowledged' do

        let(:write_concern) do
          Mongo::WriteConcern.get(w: 1)
        end

        context 'when the insert fails' do
    
          it 'aborts after first error' do
            failing_insert.execute(authorized_primary)
            expect(authorized_collection.find.count).to eq(1)
          end
        end
      end

      context 'when write concern is unacknowledged' do

        let(:write_concern) do
          Mongo::WriteConcern.get(w: 0)
        end

        context 'when the insert fails' do

          it 'aborts after first error' do
            failing_insert.execute(authorized_primary)
            expect(authorized_collection.find.count).to eq(1)
          end
        end
      end
    end

    context 'when the inserts are unordered' do

      let(:documents) do
        [{ name: 'test' }, { name: 'test' }, { name: 'test1' }]
      end

      let(:spec) do
        { documents: documents,
          db_name: authorized_collection.database.name,
          coll_name: authorized_collection.name,
          write_concern: write_concern,
          ordered: false
        }
      end

      let(:failing_insert) do
        described_class.new(spec)
      end

      context 'when write concern is acknowledged' do

        context 'when the insert fails' do
    
          it 'does not abort after first error' do
            failing_insert.execute(authorized_primary)
            expect(authorized_collection.find.count).to eq(2)
          end
        end
      end

      context 'when write concern is unacknowledged' do

        let(:write_concern) do
          Mongo::WriteConcern.get(w: 0)
        end

        context 'when the insert fails' do

          it 'does not after first error' do
            failing_insert.execute(authorized_primary)
            expect(authorized_collection.find.count).to eq(2)
          end
        end
      end
    end
  end
end
