require 'spec_helper'

describe Mongo::Operation::Write::Insert do

  let(:documents) do
    [{ :name => 'test' }]
  end

  let(:spec) do
    { :documents     => documents,
      :db_name       => TEST_DB,
      :coll_name     => TEST_COLL,
      :write_concern => Mongo::WriteConcern::Mode.get(:w => 1)
    }
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

  describe '#dup' do

    context 'deep copy' do

      it 'copies the list of documents' do
        copy = insert.dup
        expect(copy.spec[:documents]).to_not be(insert.spec[:documents])
      end
    end
  end

  describe '#execute' do

    before do
      authorized_collection.indexes.ensure({ name: 1 }, { unique: true })
    end

    after do
      authorized_collection.find.remove_many
      authorized_collection.indexes.drop({ name: 1 })
    end

    context 'when inserting a single document' do

      context 'when the insert succeeds' do

        let(:response) do
          insert.execute(authorized_primary.context)
        end

        it 'inserts the documents into the database', if: write_command_enabled? do
          expect(response.written_count).to eq(1)
        end

        it 'inserts the documents into the database', unless: write_command_enabled? do
          expect(response.written_count).to eq(0)
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
            :write_concern => Mongo::WriteConcern::Mode.get(:w => 1)
          }
        end

        let(:failing_insert) do
          described_class.new(spec)
        end

        it 'raises an error' do
          expect {
            failing_insert.execute(authorized_primary.context)
            failing_insert.execute(authorized_primary.context)
          }.to raise_error(Mongo::Operation::Write::Failure)
        end
      end
    end

    context 'when inserting multiple documents' do

      context 'when the insert succeeds' do

        let(:documents) do
          [{ name: 'test1' }, { name: 'test2' }]
        end

        let(:response) do
          insert.execute(authorized_primary.context)
        end

        it 'inserts the documents into the database', if: write_command_enabled? do
          expect(response.written_count).to eq(2)
        end

        it 'inserts the documents into the database', unless: write_command_enabled? do
          expect(response.written_count).to eq(0)
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
            :write_concern => Mongo::WriteConcern::Mode.get(:w => 1)
          }
        end

        let(:failing_insert) do
          described_class.new(spec)
        end

        it 'raises an error' do
          expect {
            failing_insert.execute(authorized_primary.context)
            failing_insert.execute(authorized_primary.context)
          }.to raise_error(Mongo::Operation::Write::Failure)
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
            :write_concern => Mongo::WriteConcern::Mode.get(:w => 1)
          }
        end

        let(:failing_insert) do
          described_class.new(spec)
        end

        it 'raises an error' do
          expect {
            failing_insert.execute(authorized_primary.context)
            failing_insert.execute(authorized_primary.context)
          }.to raise_error(Mongo::Operation::Write::Failure)
        end
      end

      context 'when a document exceeds max bson size' do

        let(:documents) do
          [{ :x => 'y'* 17000000 }]
        end

        it 'raises an error' do
          expect {
            insert.execute(authorized_primary.context)
          }.to raise_error(Mongo::Protocol::Serializers::Document::InvalidBSONSize)
        end

        it 'does not insert the document' do
          expect {
            insert.execute(authorized_primary.context)
          }.to raise_error(Mongo::Protocol::Serializers::Document::InvalidBSONSize)
          expect(authorized_collection.find.count).to eq(0)
        end
      end
    end

    context 'when the server is a secondary' do

      pending 'it raises an exception'
    end
  end
end
