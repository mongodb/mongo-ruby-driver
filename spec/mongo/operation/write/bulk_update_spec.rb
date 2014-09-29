require 'spec_helper'

describe Mongo::Operation::Write::BulkUpdate do
  include_context 'operation'

  let(:documents) do
    [{ :q => { :foo => 1 },
       :u => { :$set => { :bar => 1 } },
       :multi => true,
       :upsert => false }]
  end

  let(:spec) do
    { updates: documents,
      db_name: db_name,
      coll_name: coll_name,
      write_concern: write_concern,
      ordered: true
    }
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
            db_name: db_name,
            coll_name: coll_name,
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

  describe '#dup' do

    context 'deep copy' do

      it 'copies the list of updates' do
        copy = op.dup
        expect(copy.spec[:updates]).not_to be(op.spec[:updates])
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

    context 'when number of updates is evenly divisible by number of batches' do
      let(:documents) do
        [{ q: { a: 1 },
           u: { :$set => { a: 2 } },
           multi: true,
           upsert: false },
         { q: { b: 1 },
           u: { :$set => { b: 2 } },
           multi: true,
           upsert: false },
         { q: { c: 1 },
           u: { :$set => { c: 2 } },
           multi:  true,
           upsert: false },
         { q: { d: 1 },
           u: { :$set => { d: 2 } },
           multi: true,
           upsert: false },
         { q: { e: 1 },
           u: { :$set => { e: 2 } },
           multi: true,
           upsert: false },
         { q: { f: 1 },
           u: { :$set => { f: 2 } },
           multi:  true,
           upsert: false }
        ]
      end

      let(:n_batches) { 3 }

      it 'splits the op into the n_batches number of children ops' do
        expect(op.batch(n_batches).size).to eq(n_batches)
      end

      it 'divides the updates evenly between children ops' do
        ops = op.batch(n_batches)
        batch_size = documents.size / n_batches

        n_batches.times do |i|
          start_index = i * batch_size
          expect(ops[i].spec[:updates]).to eq(documents[start_index, batch_size])
        end
      end
    end

    context 'when number of updates is less than number of batches' do
      let(:documents) do
        [ { q: { a: 1 },
            u: { :$set => { a: 2 } } } ]
      end
      let(:n_batches) { 3 }

      it 'raises an exception' do
        expect {
            op.batch(n_batches)
          }.to raise_error(Exception)
      end
    end

    context 'when number of updates is not evenly divisible by number of batches' do
      let(:documents) do
        [{ q: { a: 1 },
           u: { :$set => { a: 2 } },
           multi: true,
           upsert: false },
         { q: { b: 1 },
           u: { :$set => { b: 2 } },
           multi: true,
           upsert: false },
         { q: { c: 1 },
           u: { :$set => { c: 2 } },
           multi:  true,
           upsert: false },
         { q: { d: 1 },
           u: { :$set => { d: 2 } },
           multi: true,
           upsert: false },
         { q: { e: 1 },
           u: { :$set => { e: 2 } },
           multi: true,
           upsert: false },
         { q: { f: 1 },
           u: { :$set => { f: 2 } },
           multi:  true,
           upsert: false }
        ]
      end
      let(:n_batches) { 4 }

      it 'splits the op into the n_batches number of children ops' do
        expect(op.batch(n_batches).size).to eq(n_batches)
      end

      it 'divides the updates evenly between children ops' do
        ops = op.batch(n_batches)
        batch_size = documents.size / n_batches

        n_batches.times do |i|
          start_index = i * batch_size
          if i == n_batches - 1
            expect(ops[i].spec[:updates]).to eq(documents[start_index..-1])
          else
            expect(ops[i].spec[:updates]).to eq(documents[start_index, batch_size])
          end
        end
      end
    end
  end

  describe '#merge!' do

    context 'same collection and database' do

      let(:other_docs) do
        [ { q: { foo: 1 },
            u: { :$set => { bar: 1 } },
            multi: true,
            upsert: true } ]
      end

      let(:other_spec) do
        { updates: other_docs,
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
        [ { q: { :foo => 1 },
            u: { :$set => { bar: 1 } },
            multi: true,
            upsert: true } ]
      end

      let(:other_spec) do
        { updates: other_docs,
          db_name: 'different',
          coll_name: coll_name
        }
      end

      let(:other) { described_class.new(other_spec) }

      it 'raises an exception' do
        expect{ op.merge!(other) }.to raise_exception
      end
    end

    context 'different collection' do

      let(:other_docs) do
        [ { q: { foo: 1 },
            u: { :$set => { bar: 1 } },
            multi: true,
            upsert: true } ]
      end

      let(:other_spec) do
        { updates: other_docs,
          db_name: db_name,
          coll_name: 'different'
        }
      end

      let(:other) { described_class.new(other_spec) }

      it 'raises an exception' do
        expect{ op.merge!(other) }.to raise_exception
      end
    end

    context 'different operation type' do
      let(:other) { Mongo::Write::Insert.new(spec) }

      it 'raises an exception' do
        expect{ op.merge!(other) }.to raise_exception
      end
    end

    context 'merged updates' do

      let(:other_docs) do
        [ { q: { foo: 1 },
            u: { :$set => { bar: 1 } },
            multi: true,
            upsert: true } ]
      end

      let(:other_spec) do
        { updates: other_docs,
          db_name: db_name,
          coll_name: coll_name
        }
      end

      let(:other) { described_class.new(other_spec) }

      let(:expected) do
        documents + other_docs
      end

      it 'merges the list of updates' do
        expect(op.merge!(other).spec[:updates]).to eq(expected)
      end
    end

    context 'mutability' do

      let(:other_docs) do
        [ { q: { foo: 1 },
            u: { :$set => { bar: 1 } },
            multi: true,
            upsert: true } ]
      end

      let(:other_spec) do
        { updates: other_docs,
          db_name: db_name,
          coll_name: coll_name
        }
      end

      let(:other) { described_class.new(other_spec) }

      it 'mutates the operation itself' do
        expect(op.merge!(other)).to be(op)
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

      context 'when the update passes' do

        let(:documents) do
          [{ q: { other: 'test' }, u: { '$set' => { field: 'blah' }}, multi: false }]
        end

        it 'updates the document' do
          op.execute(authorized_primary.context)
          expect(authorized_collection.find(field: 'blah').count).to eq(1)
        end
      end
    end

    context 'when updating multiple documents' do

      let(:update) do
        described_class.new({
          updates: documents,
          db_name: db_name,
          coll_name: coll_name,
          write_concern: write_concern
        })
      end

      context 'when the updates succeed' do

        let(:documents) do
          [{ q: { other: 'test' }, u: { '$set' => { field: 'blah' }}, multi: true }]
        end

        it 'updates the documents' do
          op.execute(authorized_primary.context)
          expect(authorized_collection.find(field: 'blah').count).to eq(2)
        end
      end
    end

    context 'when the updates are ordered' do

      let(:documents) do
        [ { q: { name: 'test' }, u: { '$st' => { field: 'blah' }}, multi: true},
          { q: { field: 'test' }, u: { '$set' => { other: 'blah' }}, multi: true }
        ]
      end

      let(:spec) do
        { updates: documents,
          db_name: db_name,
          coll_name: coll_name,
          write_concern: write_concern,
          ordered: true
        }
      end

      let(:failing_update) do
        described_class.new(spec)
      end

      context 'when the update fails' do

        context 'when write concern is acknowledged' do

          let(:write_concern) do
            Mongo::WriteConcern::Mode.get(w: 1)
          end

          it 'aborts after first error' do
            failing_update.execute(authorized_primary.context)
            expect(authorized_collection.find(other: 'blah').count).to eq(0)
          end
        end

        context 'when write concern is unacknowledged' do

          let(:write_concern) do
            Mongo::WriteConcern::Mode.get(w: 0)
          end

          it 'aborts after first error' do
            failing_update.execute(authorized_primary.context)
            expect(authorized_collection.find(other: 'blah').count).to eq(0)
          end
        end
      end
    end

    context 'when the updates are unordered' do

      let(:documents) do
        [ { q: { name: 'test' }, u: { '$st' => { field: 'blah' }}, multi: true},
          { q: { field: 'test' }, u: { '$set' => { other: 'blah' }}, multi: false }
        ]
      end

      let(:spec) do
        { updates: documents,
          db_name: db_name,
          coll_name: coll_name,
          write_concern: write_concern,
          ordered: false
        }
      end

      let(:failing_update) do
        described_class.new(spec)
      end

      context 'when the update fails' do

        context 'when write concern is acknowledged' do

          let(:write_concern) do
            Mongo::WriteConcern::Mode.get(w: 1)
          end

          it 'does not abort after first error' do
            failing_update.execute(authorized_primary.context)
            expect(authorized_collection.find(other: 'blah').count).to eq(1)
          end
        end

        context 'when write concern is unacknowledged' do

          let(:write_concern) do
            Mongo::WriteConcern::Mode.get(w: 0)
          end

          it 'does not abort after first error' do
            failing_update.execute(authorized_primary.context)
            expect(authorized_collection.find(other: 'blah').count).to eq(1)
          end
        end
      end
    end

    context 'when a write concern override is specified' do

      let(:op) do
        described_class.new({
          updates: documents,
          db_name: db_name,
          coll_name: coll_name,
          write_concern: Mongo::WriteConcern::Mode.get(w: 1),
          ordered: false
        })
      end

      let(:documents) do
        [ { q: { name: 'test' }, u: { '$st' => { field: 'blah' }}, multi: true} ]
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