require 'spec_helper'

describe Mongo::Operation::Write::Insert do

  let(:documents) do
    [{ '_id' => 1,
       'name' => 'test' }]
  end

  let(:spec) do
    { :documents     => documents,
      :db_name       => TEST_DB,
      :coll_name     => TEST_COLL,
      :write_concern => Mongo::WriteConcern.get(:w => 1)
    }
  end

  after do
    authorized_collection.delete_many
  end

  let(:insert) do
    described_class.new(spec)
  end

  describe '#initialize' do

    context 'spec' do

      it 'sets the spec' do
        expect(insert.spec).to eq(spec)
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
          expect(insert).to eq(other)
        end
      end

      context 'when two inserts have different specs' do

        let(:other_docs) do
          [{ :bar => 1 }]
        end

        let(:other_spec) do
          { :documents     => other_docs,
            :db_name       => 'test',
            :coll_name     => 'test_coll',
            :write_concern => { 'w' => 1 }
          }
        end

        let(:other) do
          described_class.new(other_spec)
        end

        it 'returns false' do
          expect(insert).not_to eq(other)
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
        insert.execute(authorized_primary.context).inserted_ids
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

        let!(:response) do
          insert.execute(authorized_primary.context)
        end

        it 'reports the correct written count', if: write_command_enabled? do
          expect(response.written_count).to eq(1)
        end

        it 'reports the correct written count', unless: write_command_enabled? do
          expect(response.written_count).to eq(0)
        end

        it 'inserts the document into the collection' do
          expect(authorized_collection.find(_id: 1).to_a). to eq(documents)
        end
      end

      context 'when the insert fails' do

        let(:documents) do
          [{ name: 'test' }]
        end

        let(:spec) do
          { :documents     => documents,
            :db_name       => TEST_DB,
            :coll_name     => TEST_COLL,
            :write_concern => Mongo::WriteConcern.get(:w => 1)
          }
        end

        let(:failing_insert) do
          described_class.new(spec)
        end

        it 'raises an error' do
          expect {
            failing_insert.execute(authorized_primary.context)
            failing_insert.execute(authorized_primary.context)
          }.to raise_error(Mongo::Error::OperationFailure)
        end
      end
    end

    context 'when inserting multiple documents' do

      context 'when the insert succeeds' do

        let(:documents) do
          [{ '_id' => 1,
             'name' => 'test1' },
           { '_id' => 2,
             'name' => 'test2' }]
        end

        let!(:response) do
          insert.execute(authorized_primary.context)
        end

        it 'reports the correct written count', if: write_command_enabled? do
          expect(response.written_count).to eq(2)
        end

        it 'reports the correct written count', unless: write_command_enabled? do
          expect(response.written_count).to eq(0)
        end

        it 'inserts the documents into the collection' do
          expect(authorized_collection.find.to_a). to eq(documents)
        end
      end

      context 'when the insert fails on the last document' do

        let(:documents) do
          [{ name: 'test3' }, { name: 'test' }]
        end

        let(:spec) do
          { :documents     => documents,
            :db_name       => TEST_DB,
            :coll_name     => TEST_COLL,
            :write_concern => Mongo::WriteConcern.get(:w => 1)
          }
        end

        let(:failing_insert) do
          described_class.new(spec)
        end

        it 'raises an error' do
          expect {
            failing_insert.execute(authorized_primary.context)
            failing_insert.execute(authorized_primary.context)
          }.to raise_error(Mongo::Error::OperationFailure)
        end
      end

      context 'when the insert fails on the first document' do

        let(:documents) do
          [{ name: 'test' }, { name: 'test4' }]
        end

        let(:spec) do
          { :documents     => documents,
            :db_name       => TEST_DB,
            :coll_name     => TEST_COLL,
            :write_concern => Mongo::WriteConcern.get(:w => 1)
          }
        end

        let(:failing_insert) do
          described_class.new(spec)
        end

        it 'raises an error' do
          expect {
            failing_insert.execute(authorized_primary.context)
            failing_insert.execute(authorized_primary.context)
          }.to raise_error(Mongo::Error::OperationFailure)
        end

      end

      context 'when a document exceeds max bson size' do

        let(:documents) do
          [{ :x => 'y'* 17000000 }]
        end

        it 'raises an error' do
          expect {
            insert.execute(authorized_primary.context)
          }.to raise_error(Mongo::Error::MaxBSONSize)
        end

        it 'does not insert the document' do
          expect {
            insert.execute(authorized_primary.context)
          }.to raise_error(Mongo::Error::MaxBSONSize)
          expect(authorized_collection.find.count).to eq(0)
        end
      end
    end
  end
end
