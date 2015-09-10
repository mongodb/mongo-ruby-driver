require 'spec_helper'

describe Mongo::Operation::Write::Bulk::Delete do
  include_context 'operation'

  let(:documents) do
    [ { 'q' => { foo: 1 }, 'limit' => 1 } ]
  end

  let(:spec) do
    { :deletes       => documents,
      :db_name       => db_name,
      :coll_name     => coll_name,
      :write_concern => write_concern,
      :ordered       => true
    }
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
          [ { 'q' => { bar: 1 }, 'limit' => 1 } ]
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

      let(:op) do
        described_class.new({
          deletes: documents,
          db_name: TEST_DB,
          coll_name: TEST_COLL,
          write_concern: Mongo::WriteConcern.get(w: 1)
        })
      end

      context 'when the delete succeeds' do

        let(:documents) do
          [{ 'q' => { field: 'test' }, 'limit' => 1 }]
        end

        it 'deletes the document from the database' do
          op.execute(authorized_primary.context)
          expect(authorized_collection.find.count).to eq(1)
        end
      end
    end

    context 'when deleting multiple documents' do

      let(:op) do
        described_class.new({
          deletes: documents,
          db_name: TEST_DB,
          coll_name: TEST_COLL,
          write_concern: Mongo::WriteConcern.get(w: 1)
        })
      end

      context 'when the deletes succeed' do

        let(:documents) do
          [{ 'q' => { field: 'test' }, 'limit' => 0 }]
        end

        it 'deletes the documents from the database' do
          op.execute(authorized_primary.context)
          expect(authorized_collection.find.count).to eq(0)
        end
      end
    end

    context 'when the deletes are ordered' do

      let(:documents) do
        [ failing_delete_doc,
          { 'q' => { field: 'test' }, 'limit' => 1 }
        ]
      end

      let(:spec) do
        { :deletes       => documents,
          :db_name       => TEST_DB,
          :coll_name     => TEST_COLL,
          :write_concern => write_concern,
          :ordered       => true
        }
      end

      let(:failing_delete) do
        described_class.new(spec)
      end

      context 'when the delete fails' do

        context 'when write concern is acknowledged' do

          let(:write_concern) do
            Mongo::WriteConcern.get(w: 1)
          end

          it 'aborts after first error' do
            failing_delete.execute(authorized_primary.context)
            expect(authorized_collection.find.count).to eq(2)
          end
        end

        context 'when write concern is unacknowledged' do

          let(:write_concern) do
            Mongo::WriteConcern.get(w: 0)
          end

          it 'aborts after first error' do
            failing_delete.execute(authorized_primary.context)
            expect(authorized_collection.find.count).to eq(2)
          end
        end
      end
    end

    context 'when the deletes are unordered' do

      let(:documents) do
        [ failing_delete_doc,
          { 'q' => { field: 'test' }, 'limit' => 1 }
        ]
      end

      let(:spec) do
        { :deletes       => documents,
          :db_name       => TEST_DB,
          :coll_name     => TEST_COLL,
          :write_concern => write_concern,
          :ordered       => false
        }
      end

      let(:failing_delete) do
        described_class.new(spec)
      end

      context 'when the delete fails' do

        context 'when write concern is acknowledged' do

          let(:write_concern) do
            Mongo::WriteConcern.get(w: 1)
          end

          it 'does not abort after first error' do
            failing_delete.execute(authorized_primary.context)
            expect(authorized_collection.find.count).to eq(1)
          end
        end

        context 'when write concern is unacknowledged' do

          let(:write_concern) do
            Mongo::WriteConcern.get(w: 0)
          end

          it 'does not abort after first error' do
            failing_delete.execute(authorized_primary.context)
            sleep(1)
            expect(authorized_collection.find.count).to eq(1)
          end
        end
      end
    end
  end
end
