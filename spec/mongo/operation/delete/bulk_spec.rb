# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Operation::Delete do
  require_no_required_api_version

  let(:context) { Mongo::Operation::Context.new }

  let(:documents) do
    [ { 'q' => { foo: 1 }, 'limit' => 1 } ]
  end

  let(:spec) do
    { :deletes       => documents,
      :db_name       => SpecConfig.instance.test_db,
      :coll_name     => TEST_COLL,
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
            :db_name       => SpecConfig.instance.test_db,
            :coll_name     => TEST_COLL,
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

  describe '#bulk_execute' do

    before do
      begin
        authorized_collection.delete_many
      rescue Mongo::Error::OperationFailure
      end
      begin
        authorized_collection.indexes.drop_all
      rescue Mongo::Error::OperationFailure
      end

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
          db_name: SpecConfig.instance.test_db,
          coll_name: TEST_COLL,
          write_concern: Mongo::WriteConcern.get(w: 1)
        })
      end

      context 'when the delete succeeds' do

        let(:documents) do
          [{ 'q' => { field: 'test' }, 'limit' => 1 }]
        end

        it 'deletes the document from the database' do
          authorized_primary.with_connection do |connection|
            op.bulk_execute(connection, context: context)
          end
          expect(authorized_collection.find.count).to eq(1)
        end
      end
    end

    context 'when deleting multiple documents' do

      let(:op) do
        described_class.new({
          deletes: documents,
          db_name: SpecConfig.instance.test_db,
          coll_name: TEST_COLL,
        })
      end

      context 'when the deletes succeed' do

        let(:documents) do
          [{ 'q' => { field: 'test' }, 'limit' => 0 }]
        end

        it 'deletes the documents from the database' do
          authorized_primary.with_connection do |connection|
            op.bulk_execute(connection, context: context)
          end
          expect(authorized_collection.find.count).to eq(0)
        end
      end
    end

    context 'when the deletes are ordered' do

      let(:documents) do
        [ { q: { '$set' => { a: 1 } }, limit: 0 },
          { 'q' => { field: 'test' }, 'limit' => 1 }
        ]
      end

      let(:spec) do
        { :deletes       => documents,
          :db_name       => SpecConfig.instance.test_db,
          :coll_name     => TEST_COLL,
          :ordered       => true
        }
      end

      let(:failing_delete) do
        described_class.new(spec)
      end

      context 'when the delete fails' do

        context 'when write concern is acknowledged' do

          let(:write_concern) do
            Mongo::WriteConcern.get(w: :majority)
          end

          it 'aborts after first error' do
            authorized_primary.with_connection do |connection|
              failing_delete.bulk_execute(connection, context: context)
            end
            expect(authorized_collection.find.count).to eq(2)
          end
        end

        context 'when write concern is unacknowledged' do

          let(:write_concern) do
            Mongo::WriteConcern.get(w: 0)
          end

          it 'aborts after first error' do
            authorized_primary.with_connection do |connection|
              failing_delete.bulk_execute(connection, context: context)
            end
            expect(authorized_collection.find.count).to eq(2)
          end
        end
      end
    end

    context 'when the deletes are unordered' do

      let(:documents) do
        [ { q: { '$set' => { a: 1 } }, limit: 0 },
          { 'q' => { field: 'test' }, 'limit' => 1 }
        ]
      end

      let(:spec) do
        { :deletes       => documents,
          :db_name       => SpecConfig.instance.test_db,
          :coll_name     => TEST_COLL,
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
            authorized_primary.with_connection do |connection|
              failing_delete.bulk_execute(connection, context: context)
            end
            expect(authorized_collection.find.count).to eq(1)
          end
        end

        context 'when write concern is unacknowledged' do

          let(:write_concern) do
            Mongo::WriteConcern.get(w: 0)
          end

          it 'does not abort after first error' do
            authorized_primary.with_connection do |connection|
              failing_delete.bulk_execute(connection, context: context)
            end
            expect(authorized_collection.find.count).to eq(1)
          end
        end
      end
    end
  end
end
