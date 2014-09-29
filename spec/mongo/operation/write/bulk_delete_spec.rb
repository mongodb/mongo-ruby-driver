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

  describe '#write_concern=' do

    let(:other_write_concern) do
      Mongo::WriteConcern::Mode.get(:w => 2)
    end

    context 'when the write concern is set' do

      it 'sets the write concern' do
        new_op = op.write_concern(other_write_concern)
        expect(new_op.write_concern).to eq(other_write_concern)
      end
    end
  end

  describe '#batch' do

    context 'when number of deletes is evenly divisible by n_batches' do
      let(:documents) do
        [ { q: { a: 1 } },
          { q: { b: 1 } },
          { q: { c: 1 } },
          { q: { d: 1 } },
          { q: { e: 1 } },
          { q: { f: 1 } } ]
      end
      let(:n_batches) { 3 }

      it 'batches the op into the divisor number of children ops' do
        expect(op.batch(n_batches).size).to eq(n_batches)
      end

      it 'divides the deletes evenly between children ops' do
        ops = op.batch(n_batches)
        batch_size = documents.size / n_batches

        n_batches.times do |i|
          start_index = i * batch_size
          expect(ops[i].spec[:deletes]).to eq(documents[start_index, batch_size])
        end
      end
    end

    context 'when number of deletes is less than batch size' do
      let(:documents) do
        [ { q: { a: 1 } } ]
      end
      let(:n_batches) { 3 }

      it 'raises an exception' do
        expect {
            op.batch(n_batches)
          }.to raise_error(Exception)
      end
    end

    context 'when number of deletes is not evenly divisible by n_batches' do
      let(:documents) do
        [ { q: { a: 1 } },
          { q: { b: 1 } },
          { q: { c: 1 } },
          { q: { d: 1 } },
          { q: { e: 1 } },
          { q: { f: 1 } } ]
      end
      let(:n_batches) { 4 }

      it 'batches the op into the n_batches number of children ops' do
        expect(op.batch(n_batches).size).to eq(n_batches)
      end

      it 'divides the deletes evenly between children ops' do
        ops = op.batch(n_batches)
        batch_size = documents.size / n_batches

        n_batches.times do |i|
          start_index = i * batch_size
          if i == n_batches - 1
            expect(ops[i].spec[:deletes]).to eq(documents[start_index..-1])
          else
            expect(ops[i].spec[:deletes]).to eq(documents[start_index, batch_size])
          end
        end
      end
    end
  end

  describe '#merge!' do

    context 'same collection and database' do

      let(:other_docs) do
        [ { q: { bar: 1 }, limit: 1 } ]
      end

      let(:other_spec) do
        { deletes: other_docs,
          db_name: db_name,
          coll_name: coll_name
        }
      end

      let(:other) { described_class.new(other_spec) }

      it 'merges the two ops' do
        expect{ op.merge!(other) }.not_to raise_exception
      end
    end

    context 'different database' do

      let(:other_docs) do
        [ { q: { bar: 1 }, limit: 1 } ]
      end

      let(:other_spec) do
        { deletes: other_docs,
          db_name: 'different',
          coll_name: coll_name
        }
      end

      let(:other) { described_class.new(other_spec) }

      it 'raises an exception' do
        expect do
          op.merge!(other)
        end.to raise_exception
      end
    end

    context 'different collection' do

      let(:other_docs) do
        [ { q: { bar: 1 }, limit: 1 } ]
      end

      let(:other_spec) do
        { deletes: other_docs,
          db_name: db_name,
          coll_name: 'different'
        }
      end

      let(:other) { described_class.new(other_spec) }

      it 'raises an exception' do
        expect do
          op.merge!(other)
        end.to raise_exception
      end
    end

    context 'different operation type' do
      let(:other) { Mongo::Write::Update.new(spec) }

      it 'raises an exception' do
        expect do
          op.merge!(other)
        end.to raise_exception
      end
    end

    context 'merged deletes' do

      let(:other_docs) do
        [ { q: { bar: 1 }, limit: 1 } ]
      end

      let(:other_spec) do
        { deletes: other_docs,
          db_name: db_name,
          coll_name: coll_name
        }
      end

      let(:other) { described_class.new(other_spec) }

      let(:expected) do
        documents + other_docs
      end

      it 'merges the list of deletes' do
        expect(op.merge!(other).spec[:deletes]).to eq(expected)
      end
    end

    context 'mutability' do
      let(:other_docs) do
        [ { q: { bar: 1 }, limit: 1 } ]
      end

      let(:other_spec) do
        { deletes: other_docs,
          db_name: db_name,
          coll_name: coll_name
        }
      end

      let(:other) { described_class.new(other_spec) }

      it 'returns a new object' do
        expect(op.merge!(other)).to be(op)
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

      let(:op) do
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
          write_concern: Mongo::WriteConcern::Mode.get(w: 1)
        })
      end

      context 'when the deletes succeed' do

        let(:documents) do
          [{ q: { field: 'test' }, limit: 0 }]
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
          { q: { field: 'test' }, limit: 1 }
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
            Mongo::WriteConcern::Mode.get(w: 1)
          end
        
          it 'aborts after first error' do
            failing_delete.execute(authorized_primary.context)
            expect(authorized_collection.find.count).to eq(2)
          end
        end

        context 'when write concern is unacknowledged' do
          
          let(:write_concern) do
            Mongo::WriteConcern::Mode.get(w: 0)
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
          { q: { field: 'test' }, limit: 1 }
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
            Mongo::WriteConcern::Mode.get(w: 1)
          end
        
          it 'does not abort after first error' do
            failing_delete.execute(authorized_primary.context)
            expect(authorized_collection.find.count).to eq(1)
          end
        end

        context 'when write concern is unacknowledged' do
          
          let(:write_concern) do
            Mongo::WriteConcern::Mode.get(w: 0)
          end
      
          it 'does not abort after first error' do
            failing_delete.execute(authorized_primary.context)
            expect(authorized_collection.find.count).to eq(1)
          end
        end
      end
    end

    context 'when a write concern override is specified' do

      let(:documents) do
        [ { q: { field: 'test' }, limit: 1 } ]
      end

      let(:op) do
        described_class.new({
            deletes: documents,
            db_name: db_name,
            coll_name: coll_name,
            write_concern: Mongo::WriteConcern::Mode.get(w: 1),
            ordered: false
          })
      end

      let(:unacknowledged) do
        Mongo::WriteConcern::Mode.get(w: 0)
      end

      it 'uses that write concern', if: write_command_enabled? do
        result = op.write_concern(unacknowledged).execute(authorized_primary.context)
        expect(result.replies.size).to eq(1)
      end

      it 'uses that write concern', unless: write_command_enabled? do
        result = op.write_concern(unacknowledged).execute(authorized_primary.context)
        expect(result.replies).to be(nil)
      end
    end

    context 'when the server is a secondary' do

      pending 'it raises an exception'
    end
  end
end
