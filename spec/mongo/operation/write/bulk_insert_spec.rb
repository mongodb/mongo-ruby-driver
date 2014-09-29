require 'spec_helper'

describe Mongo::Operation::Write::BulkInsert do
  include_context 'operation'

  let(:documents) do
    [{ :name => 'test' }]
  end

  let(:spec) do
    { documents: documents,
      db_name: db_name,
      coll_name: coll_name,
      write_concern: write_concern
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

      context 'when two inserts have the same specs' do

        let(:other) do
          described_class.new(spec)
        end

        it 'returns true' do
          expect(op).to eq(other)
        end
      end

      context 'when two inserts have different specs' do

        let(:other_docs) do
          [{ :bar => 1 }]
        end

        let(:other_spec) do
          { :documents     => other_docs,
            :db_name       => 'test',
            :coll_name     => 'coll_name',
            :write_concern => { 'w' => 1 },
            :ordered       => true
          }
        end

        let(:other) do
          described_class.new(other_spec)
        end

        it 'returns false' do
          expect(op).not_to eq(other)
        end
      end
    end
  end

  describe '#dup' do

    context 'deep copy' do

      it 'copies the list of documents' do
        copy = op.dup
        expect(copy.spec[:documents]).to_not be(op.spec[:documents])
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

    context 'when number of inserts is evenly divisible by number of batches' do
      let(:documents) do
        [ { a: 1 },
          { b: 1 },
          { c: 1 },
          { d: 1 },
          { e: 1 },
          { f: 1 } ]
      end
      let(:n_batches) { 3 }

      it 'batches the insert into the n_batches number of children inserts' do
        expect(op.batch(n_batches).size).to eq(n_batches)
      end

      it 'divides the inserts evenly between children inserts' do
        inserts = op.batch(n_batches)
        batch_size = documents.size / n_batches
    
        n_batches.times do |i|
          start_index = i * batch_size
          expect(inserts[i].spec[:documents]).to eq(documents[start_index, batch_size])
        end
      end
    end

    context 'when number of documents is less than number of batches' do
      let(:documents) do
        [ { a: 1 } ]
      end
      let(:n_batches) { 3 }

      it 'raises an exception' do
        expect {
            op.batch(n_batches)
          }.to raise_error(Exception)
      end
    end

    context 'when number of inserts is not evenly divisible by number of batches' do
      let(:documents) do
        [ { a: 1 },
          { b: 1 },
          { c: 1 },
          { d: 1 },
          { e: 1 },
          { f: 1 } ]
      end
      let(:n_batches) { 4 }

      it 'batches the insert into the n_batches number of children inserts' do
        expect(op.batch(n_batches).size).to eq(n_batches)
      end

      it 'batches the inserts evenly between children inserts' do
        inserts = op.batch(n_batches)
        batch_size = documents.size / n_batches
    
        n_batches.times do |i|
          start_index = i * batch_size
          if i == n_batches - 1
            expect(inserts[i].spec[:documents]).to eq(documents[start_index..-1])
          else
            expect(inserts[i].spec[:documents]).to eq(documents[start_index, batch_size])
          end
        end
      end
    end
  end

  describe '#merge!' do

    context 'when collection and database are the same' do

      let(:other_docs) do
        [ { bar: 1 } ]
      end

      let(:other_spec) do
        { :documents     => other_docs,
          :db_name       => db_name,
          :coll_name     => coll_name
        }
      end

      let(:other) do
        described_class.new(other_spec)
      end

      it 'merges the two inserts' do
        expect do
          op.merge!(other)
        end.not_to raise_exception
      end
    end

    context 'when the database differs' do

      let(:other_docs) do
        [ { bar: 1 } ]
      end

      let(:other_spec) do
        { :documents     => other_docs,
          :db_name       => 'different',
          :coll_name     => coll_name
        }
      end

      let(:other) do
        described_class.new(other_spec)
      end

      it 'raises an exception' do
        expect do
          op.merge!(other)
        end.to raise_exception
      end
    end

    context 'when the collection differs' do

      let(:other_docs) do
        [ { bar: 1 } ]
      end

      let(:other_spec) do
        { :documents     => other_docs,
          :db_name       => db_name,
          :coll_name     => 'different'
        }
      end

      let(:other) do
        described_class.new(other_spec)
      end

      it 'raises an exception' do
        expect do
          op.merge!(other)
        end.to raise_exception
      end
    end

    context 'when the command type differs' do

      let(:other) do
        Mongo::Write::Update.new(spec)
      end

      it 'raises an exception' do
        expect do
          op.merge!(other)
        end.to raise_exception
      end
    end

    context 'when the commands can be merged' do

      let(:other_docs) do
        [ { bar: 1 } ]
      end

      let(:other_spec) do
        { :documents     => other_docs,
          :db_name       => db_name,
          :coll_name     => coll_name
        }
      end

      let(:other) do
        described_class.new(other_spec)
      end

      let(:expected) do
        documents + other_docs
      end

      it 'merges the list of documents' do
        expect(op.merge!(other).spec[:documents]).to eq(expected)
      end

      it 'mutates the original spec' do
        expect(op.merge!(other)).to be(op)
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
          op.execute(authorized_primary.context)
        end

        it 'inserts the documents into the database', if: write_command_enabled? do
          expect(response.written_count).to eq(1)
        end

        it 'inserts the documents into the database', unless: write_command_enabled? do
          expect(response.written_count).to eq(0)
        end
      end
    end

    context 'when inserting multiple documents' do

      context 'when the insert succeeds' do

        let(:documents) do
          [{ name: 'test1' }, { name: 'test2' }]
        end

        let(:response) do
          op.execute(authorized_primary.context)
        end

        it 'inserts the documents into the database', if: write_command_enabled? do
          expect(response.written_count).to eq(2)
        end

        it 'inserts the documents into the database', unless: write_command_enabled? do
          expect(response.written_count).to eq(0)
        end
      end
    end

    context 'when the inserts are ordered' do

      let(:documents) do
        [{ name: 'test' }, { name: 'test' }, { name: 'test1' }]
      end

      let(:spec) do
        { documents: documents,
          db_name: db_name,
          coll_name: coll_name,
          write_concern: write_concern,
          ordered: true
        }
      end

      let(:failing_insert) do
        described_class.new(spec)
      end

      context 'when write concern is acknowledged' do

        let(:write_concern) do
          Mongo::WriteConcern::Mode.get(w: 1)
        end

        context 'when the insert fails' do
    
          it 'aborts after first error' do
            failing_insert.execute(authorized_primary.context)
            expect(authorized_collection.find.count).to eq(1)
          end
        end
      end

      context 'when write concern is unacknowledged' do

        let(:write_concern) do
          Mongo::WriteConcern::Mode.get(w: 0)
        end

        context 'when the insert fails' do

          it 'aborts after first error' do
            failing_insert.execute(authorized_primary.context)
            expect(authorized_collection.find.count).to eq(1)
          end
        end
      end
    end

    context 'when the inserts are unordered' do

      let(:documents) do
        [{ name: 'test' }, { name: 'test' }, { name: 'test1' }]
      end

      let(:spec) do
        { documents: documents,
          db_name: db_name,
          coll_name: coll_name,
          write_concern: write_concern,
          ordered: false
        }
      end

      let(:failing_insert) do
        described_class.new(spec)
      end

      context 'when write concern is acknowledged' do

        let(:write_concern) do
          Mongo::WriteConcern::Mode.get(w: 1)
        end

        context 'when the insert fails' do
    
          it 'does not abort after first error' do
            failing_insert.execute(authorized_primary.context)
            expect(authorized_collection.find.count).to eq(2)
          end
        end
      end

      context 'when write concern is unacknowledged' do

        let(:write_concern) do
          Mongo::WriteConcern::Mode.get(w: 0)
        end

        context 'when the insert fails' do

          it 'does not after first error' do
            failing_insert.execute(authorized_primary.context)
            expect(authorized_collection.find.count).to eq(2)
          end
        end
      end
    end

    context 'when a write concern override is specified' do

      let(:op) do
        described_class.new({
            documents: documents,
            db_name: db_name,
            coll_name: coll_name,
            write_concern: Mongo::WriteConcern::Mode.get(w: 0),
            ordered: false
          })
      end

      let(:documents) do
        [{ name: 'test' }, { name: 'test' }, { name: 'test1' }]
      end

      let(:acknowledged) do
        Mongo::WriteConcern::Mode.get(w: 1)
      end

      it 'uses that write concern' do
        replies = op.write_concern(acknowledged).execute(authorized_primary.context).replies
        expect(replies.size).to eq(1)
      end
    end

    context 'when the server is a secondary' do

      pending 'it raises an exception'
    end
  end
end