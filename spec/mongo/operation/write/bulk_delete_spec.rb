require 'spec_helper'

describe Mongo::Operation::Write::BulkDelete do
  include_context 'operation'

  let(:documents) do
    [ { q: { foo: 1 }, limit: 1 } ]
  end

  let(:spec) do
    { :deletes       => documents,
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
        let(:other_docs) do
          [ { q: { bar: 1 }, limit: 1 } ]
        end

        let(:other_spec) do
          { :deletes       => other_docs,
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

  describe '#dup' do

    context 'deep copy' do

      it 'copies the list of deletes' do
        copy = op.dup
        expect(copy.spec[:deletes]).not_to be(op.spec[:deletes])
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
      authorized_collection.find.remove_many
    end

    context 'when deleting a single document' do

      let(:delete) do
        described_class.new({
          deletes: documents,
          db_name: TEST_DB,
          coll_name: TEST_COLL,
          write_concern: Mongo::WriteConcern::Mode.get(w: 1)
        })
      end

      context 'when the delete succeeds' do

        let(:documents) do
          [{ q: { field: 'test' }, limit: 1 }]
        end

        let(:result) do
          delete.execute(authorized_primary.context)
        end

        it 'deletes the documents from the database' do
          expect(result.n).to eq(1)
        end
      end

      context 'when the delete fails' do

        let(:documents) do
          [{ que: { field: 'test' }}]
        end

        it 'raises an exception' do
          expect {
            delete.execute(authorized_primary.context)
          }.to raise_error(Mongo::Operation::Write::Failure)
        end
      end
    end

    context 'when deleting multiple documents' do

      let(:delete) do
        described_class.new({
          deletes: documents,
          db_name: TEST_DB,
          coll_name: TEST_COLL,
          write_concern: Mongo::WriteConcern::Mode.get(w: 1)
        })
      end

      context 'when the deletes succeed' do

        let(:documents) do
          [{ q: { field: 'test' }, limit: -1 }]
        end

        let(:result) do
          delete.execute(authorized_primary.context)
        end

        it 'deletes the documents from the database' do
          expect(result.n).to eq(2)
        end
      end

      context 'when a delete fails' do

        let(:documents) do
          [{ q: { field: 'tester' }, limit: -1 }]
        end

        let(:result) do
          delete.execute(authorized_primary.context)
        end

        it 'does not delete any documents' do
          expect(result.n).to eq(0)
        end
      end
    end

    context 'when the deletes are ordered' do

      let(:documents) do
        [ { que: { field: 'test' }},
          { q: { field: 'test' }, limit: 1 }
        ]
      end

      let(:spec) do
        { :deletes       => documents,
          :db_name       => TEST_DB,
          :coll_name     => TEST_COLL,
          :write_concern => Mongo::WriteConcern::Mode.get(w: 1),
          :ordered       => true
        }
      end

      let(:failing_delete) do
        described_class.new(spec)
      end
  
      it 'aborts after first error' do
        expect {
          failing_delete.execute(authorized_primary.context)
        }.to raise_error(Mongo::Operation::Write::Failure)
        expect(authorized_collection.find.count).to eq(2)
      end
    end

    context 'when the deletes are unordered' do

      let(:documents) do
        [
          { q: { field: 'test' } },
          { q: { field: 'test' }, limit: 1 }
        ]
      end

      let(:spec) do
        { :deletes       => documents,
          :db_name       => TEST_DB,
          :coll_name     => TEST_COLL,
          :write_concern => Mongo::WriteConcern::Mode.get(w: 1),
          :ordered       => false
        }
      end

      let(:failing_delete) do
        described_class.new(spec)
      end
  
      pending 'it continues executing operations after errors'
    end

    context 'when the server is a secondary' do

      pending 'it raises an exception'
    end
  end
end
