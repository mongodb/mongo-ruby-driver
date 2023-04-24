# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Operation::Update do
  require_no_multi_mongos
  require_no_required_api_version

  let(:context) { Mongo::Operation::Context.new }

  let(:documents) do
    [{ :q => { :foo => 1 },
       :u => { :$set => { :bar => 1 } },
       :multi => true,
       :upsert => false }]
  end

  let(:spec) do
    { updates: documents,
      db_name: authorized_collection.database.name,
      coll_name: authorized_collection.name,
      write_concern: write_concern,
      ordered: true
    }
  end

  let(:write_concern) do
    Mongo::WriteConcern.get(w: :majority)
  end

  let(:op) do
    described_class.new(spec)
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

      context 'when two ops have the same specs' do

        let(:other) { described_class.new(spec) }

        it 'returns true' do
          expect(op).to eq(other)
        end
      end

      context 'when two ops have different specs' do
        let(:other_docs) do
          [ {:q => { :foo => 1 },
             :u => { :$set => { :bar => 1 } },
             :multi => true,
             :upsert => true } ]
        end

        let(:other_spec) do
          { updates: other_docs,
            db_name: authorized_collection.database.name,
            coll_name: authorized_collection.name,
            write_concern: write_concern,
            ordered: true
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
      authorized_collection.drop
      authorized_collection.insert_many([
        { name: 'test', field: 'test', other: 'test' },
        { name: 'testing', field: 'test', other: 'test' }
      ])
    end

    after do
      authorized_collection.delete_many
    end

    context 'when updating a single document' do

      context 'when the update passes' do

        let(:documents) do
          [{ 'q' => { other: 'test' }, 'u' => { '$set' => { field: 'blah' }}, 'multi' => false }]
        end

        it 'updates the document' do
          authorized_primary.with_connection do |connection|
            op.bulk_execute(connection, context: context)
          end
          expect(authorized_collection.find(field: 'blah').count).to eq(1)
        end
      end
    end

    context 'when updating multiple documents' do

      let(:update) do
        described_class.new({
          updates: documents,
          db_name: authorized_collection.database.name,
          coll_name: authorized_collection.name,
          write_concern: write_concern
        })
      end

      context 'when the updates succeed' do

        let(:documents) do
          [{ 'q' => { other: 'test' }, 'u' => { '$set' => { field: 'blah' }}, 'multi' => true }]
        end

        it 'updates the documents' do
          authorized_primary.with_connection do |connection|
            op.bulk_execute(connection, context: context)
          end
          expect(authorized_collection.find(field: 'blah').count).to eq(2)
        end
      end
    end

    context 'when the updates are ordered' do

      let(:documents) do
        [ { 'q' => { name: 'test' }, 'u' => { '$st' => { field: 'blah' }}, 'multi' => true},
          { 'q' => { field: 'test' }, 'u' => { '$set' => { other: 'blah' }}, 'multi' => true }
        ]
      end

      let(:spec) do
        { updates: documents,
          db_name: authorized_collection.database.name,
          coll_name: authorized_collection.name,
          write_concern: write_concern,
          ordered: true
        }
      end

      let(:failing_update) do
        described_class.new(spec)
      end

      context 'when the update fails' do

        context 'when write concern is acknowledged' do

          it 'aborts after first error' do
            authorized_primary.with_connection do |connection|
              failing_update.bulk_execute(connection, context: context)
            end
            expect(authorized_collection.find(other: 'blah').count).to eq(0)
          end
        end

        context 'when write concern is unacknowledged' do

          let(:write_concern) do
            Mongo::WriteConcern.get(w: 0)
          end

          it 'aborts after first error' do
            authorized_primary.with_connection do |connection|
              failing_update.bulk_execute(connection, context: context)
            end
            expect(authorized_collection.find(other: 'blah').count).to eq(0)
          end
        end
      end
    end

    context 'when the updates are unordered' do

      let(:documents) do
        [ { 'q' => { name: 'test' }, 'u' => { '$st' => { field: 'blah' }}, 'multi' => true},
          { 'q' => { field: 'test' }, 'u' => { '$set' => { other: 'blah' }}, 'multi' => false }
        ]
      end

      let(:spec) do
        { updates: documents,
          db_name: authorized_collection.database.name,
          coll_name: authorized_collection.name,
          write_concern: write_concern,
          ordered: false
        }
      end

      let(:failing_update) do
        described_class.new(spec)
      end

      context 'when the update fails' do

        context 'when write concern is acknowledged' do

          it 'does not abort after first error' do
            authorized_primary.with_connection do |connection|
              failing_update.bulk_execute(connection, context: context)
            end
            expect(authorized_collection.find(other: 'blah').count).to eq(1)
          end
        end

        context 'when write concern is unacknowledged' do

          let(:write_concern) do
            Mongo::WriteConcern.get(w: 0)
          end

          it 'does not abort after first error' do
            authorized_primary.with_connection do |connection|
              failing_update.bulk_execute(connection, context: context)
            end
            expect(authorized_collection.find(other: 'blah').count).to eq(1)
          end
        end
      end
    end
  end
end
