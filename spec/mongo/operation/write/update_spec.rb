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
      :write_concern => Mongo::WriteConcern.get(:w => 1),
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
            :write_concern => Mongo::WriteConcern.get(:w => 1),
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
      authorized_collection.delete_many
    end

    context 'when updating a single document' do

      let(:update) do
        described_class.new({
          update: document,
          db_name: TEST_DB,
          coll_name: TEST_COLL,
          write_concern: Mongo::WriteConcern.get(:w => 1)
        })
      end

      context 'when the update succeeds' do

        let(:document) do
          { 'q' => { name: 'test' }, 'u' => { '$set' => { field: 'blah' }} }
        end

        let(:result) do
          update.execute(authorized_primary)
        end

        it 'updates the document' do
          expect(result.written_count).to eq(1)
        end

        it 'reports the modified count', if: write_command_enabled? do
          expect(result.modified_count).to eq(1)
        end

        it 'returns nil for the modified count', unless: write_command_enabled? do
          expect(result.modified_count).to be_nil
        end

        it 'reports the matched count' do
          expect(result.matched_count).to eq(1)
        end

        it 'reports the upserted id as nil' do
          expect(result.upserted_id).to eq(nil)
        end
      end

      context 'when the update fails' do

        let(:document) do
          { 'q' => { name: 'test' }, 'u' => { '$st' => { field: 'blah' } } }
        end

        it 'raises an exception' do
          expect {
            update.execute(authorized_primary)
          }.to raise_error(Mongo::Error::OperationFailure)
        end
      end
    end

    context 'when updating multiple documents' do

      let(:update) do
        described_class.new({
          update: document,
          db_name: TEST_DB,
          coll_name: TEST_COLL,
          write_concern: Mongo::WriteConcern.get(:w => 1)
        })
      end

      context 'when the updates succeed' do

        let(:document) do
          { 'q' => { field: 'test' }, 'u' => { '$set' => { other: 'blah' }}, 'multi' => true }
        end

        let(:result) do
          update.execute(authorized_primary)
        end

        it 'updates the documents' do
          expect(result.written_count).to eq(2)
        end

        it 'reports the modified count', if: write_command_enabled? do
          expect(result.modified_count).to eq(2)
        end

        it 'returns nil for the modified count', unless: write_command_enabled? do
          expect(result.modified_count).to be_nil
        end

        it 'reports the matched count' do
          expect(result.matched_count).to eq(2)
        end

        it 'reports the upserted id as nil' do
          expect(result.upserted_id).to eq(nil)
        end
      end

      context 'when an update fails' do

        let(:document) do
          { 'q' => { name: 'test' }, 'u' => { '$st' => { field: 'blah' } }, 'multi' => true }
        end

        it 'raises an exception' do
          expect {
            update.execute(authorized_primary)
          }.to raise_error(Mongo::Error::OperationFailure)
        end
      end

      context 'when a document exceeds max bson size' do

        let(:document) do
          { 'q' => { name: 't'*17000000}, 'u' => { '$set' => { field: 'blah' } } }
        end

        it 'raises an error' do
          expect {
            update.execute(authorized_primary)
          }.to raise_error(Mongo::Error::MaxBSONSize)
        end
      end

      context 'when upsert is true' do

        let(:document) do
          { 'q' => { field: 'non-existent' }, 'u' => { '$set' => { other: 'blah' }}, 'upsert' => true }
        end

        let(:result) do
          update.execute(authorized_primary)
        end

        it 'inserts the document' do
          expect(result.written_count).to eq(1)
        end

        it 'reports the modified count', if: write_command_enabled? do
          expect(result.modified_count).to eq(0)
        end

        it 'returns nil for the modified count', unless: write_command_enabled? do
          expect(result.modified_count).to be_nil
        end

        it 'reports the matched count' do
          expect(result.matched_count).to eq(0)
        end

        it 'retruns the upserted id' do
          expect(result.upserted_id).to be_a(BSON::ObjectId)
        end
      end
    end

    context 'when write concern { w: 0 } is used' do

      let(:update) do
        described_class.new({
                                update: document,
                                db_name: TEST_DB,
                                coll_name: TEST_COLL,
                                write_concern: Mongo::WriteConcern.get(:w => 0)
                            })
      end

      let(:document) do
        { 'q' => { name: 'test' }, 'u' => { '$set' => { field: 'blah' }}, limit: 1 }
      end

      let(:result) do
        update.execute(authorized_primary)
      end

      before do
        expect(update).to receive(:execute_message).and_call_original
      end

      it 'uses op codes instead of write commands' do
        expect(result.written_count).to eq(0)
      end
    end
  end
end
