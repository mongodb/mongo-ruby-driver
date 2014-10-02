require 'spec_helper'

describe Mongo::Operation::Write::Update do

  let(:document) do
    { :q => { :foo => 1 },
      :u => { :$set => { :bar => 1 } },
      :multi => true,
      :upsert => false }
  end

  let(:spec) do
    { :update        => document,
      :db_name       => TEST_DB,
      :coll_name     => TEST_COLL,
      :write_concern => Mongo::WriteConcern::Mode.get(:w => 1),
      :ordered       => true
    }
  end

  let(:update) do
    described_class.new(spec)
  end

  describe '#initialize' do

    context 'spec' do

      it 'sets the spec' do
        expect(update.spec).to eq(spec)
      end
    end
  end

  describe '#==' do

    context 'spec' do

      context 'when two ops have the same specs' do

        let(:other) { described_class.new(spec) }

        it 'returns true' do
          expect(update).to eq(other)
        end
      end

      context 'when two ops have different specs' do
        let(:other_doc) { {:q => { :foo => 1 },
                           :u => { :$set => { :bar => 1 } },
                           :multi => true,
                           :upsert => true } }
        let(:other_spec) do
          { :update        => other_doc,
            :db_name       => TEST_DB,
            :coll_name     => TEST_COLL,
            :write_concern => Mongo::WriteConcern::Mode.get(:w => 1),
            :ordered       => true
          }
        end

        let(:other) { described_class.new(other_spec) }

        it 'returns false' do
          expect(update).not_to eq(other)
        end
      end
    end
  end

  describe '#execute' do

    before do
      authorized_collection.insert_many([
        { name: 'test', field: 'test', other: 'test' },
        { name: 'testing', field: 'test', other: 'test' }
      ])
    end

    after do
      authorized_collection.find.remove_many
    end

    context 'when updating a single document' do

      let(:update) do
        described_class.new({
          update: document,
          db_name: TEST_DB,
          coll_name: TEST_COLL,
          write_concern: Mongo::WriteConcern::Mode.get(:w => 1)
        })
      end

      context 'when the update passes' do

        let(:document) do
          { q: { name: 'test' }, u: { '$set' => { field: 'blah' }}, limit: 1 }
        end

        let(:result) do
          update.execute(authorized_primary.context)
        end

        it 'updates the document' do
          expect(result.written_count).to eq(1)
        end
      end

      context 'when the update fails' do

        let(:document) do
          { q: { name: 'test' }, u: { '$st' => { field: 'blah' } } }
        end

        it 'raises an exception' do
          expect {
            update.execute(authorized_primary.context)
          }.to raise_error(Mongo::Operation::Write::Failure)
        end
      end
    end

    context 'when updating multiple documents' do

      let(:update) do
        described_class.new({
          update: document,
          db_name: TEST_DB,
          coll_name: TEST_COLL,
          write_concern: Mongo::WriteConcern::Mode.get(:w => 1)
        })
      end

      context 'when the updates succeed' do

        let(:document) do
          { q: { field: 'test' }, u: { '$set' => { other: 'blah' }}, multi: true }
        end

        let(:result) do
          update.execute(authorized_primary.context)
        end

        it 'updates the documents' do
          expect(result.written_count).to eq(2)
        end
      end

      context 'when an update fails' do

        let(:document) do
          { q: { name: 'test' }, u: { '$st' => { field: 'blah' } }, multi: true }
        end

        it 'raises an exception' do
          expect {
            update.execute(authorized_primary.context)
          }.to raise_error(Mongo::Operation::Write::Failure)
        end
      end

      context 'when a document exceeds max bson size' do

        let(:document) do
          { q: { name: 't'*17000000}, u: { '$set' => { field: 'blah' } } }
        end

        it 'raises an error' do
          expect {
            update.execute(authorized_primary.context)
          }.to raise_error(Mongo::Protocol::Serializers::Document::InvalidBSONSize)
        end
      end
    end

    context 'when the server is a secondary' do

      pending 'it raises an exception'
    end
  end
end
