require 'spec_helper'

describe Mongo::Operation::Read::GetMore do
  include_context 'operation'

  let(:to_return) { 50 }
  let(:cursor_id) { 1 }

  let(:spec) do
    { :to_return => to_return,
      :cursor_id => cursor_id }
  end

  let(:op) { described_class.new(collection, spec) }

  describe '#initialize' do

    it 'sets the spec' do
      expect(op.spec).to be(spec)
    end

    it 'sets the collection' do
      expect(op.collection).to be(collection)
    end
  end

  describe '#==' do

    context 'spec' do

      context 'when two ops have the same specs' do
        let(:other) { described_class.new(collection, spec) }

        it 'returns true' do
          expect(op).to eq(other)
        end
      end

      context ' when two ops have different specs' do
        let(:other_spec) do
          { :to_return => 50,
            :cursor_id => 2 }
        end
        let(:other) { described_class.new(collection, other_spec) }
  
        it 'returns false' do
          expect(op).not_to eq(other)
        end
      end
    end

    context 'collection' do
      context 'when two ops have the same collection' do
        let(:other) { described_class.new(collection, spec) }

        it 'returns true' do
          expect(op).to eq(other)
        end
      end

      context 'when two ops have different collections' do
        let(:other_collection) { double('collection') }
        let(:other) { described_class.new(other_collection, spec) }

        it 'returns false' do
          expect(op).not_to eq(other)
        end
      end
    end
  end

  describe '#execute' do

    context 'message' do

      it 'creates a get more wire protocol message with correct specs' do
        expect(Mongo::Protocol::GetMore).to receive(:new) do |db, coll, to_ret, id|
          expect(db).to eq(collection.database.name)
          expect(coll).to eq(collection.name)
          expect(to_ret).to eq(to_return)
          expect(id).to eq(cursor_id)
        end
        op.execute(primary_context)
      end
    end

    context 'connection' do

      it 'dispatches the message on the connection' do
        expect(connection).to receive(:dispatch)
        op.execute(primary_context)
      end
    end
  end
end
