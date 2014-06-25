require 'spec_helper'

describe Mongo::Operation::Read::Query do
  include_context 'operation'

  let(:selector) { {} }
  let(:query_opts) { {} }
  let(:spec) do
    { :selector  => selector,
      :opts      => query_opts
    }
  end
  let(:op) { described_class.new(collection, spec) }

  describe '#initialize' do

    context 'query spec' do
      it 'sets the query spec' do
        expect(op.spec).to be(spec)
      end
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

      it 'creates a query wire protocol message with correct specs' do
        expect(Mongo::Protocol::Query).to receive(:new) do |db, coll, sel, opts|
          expect(db).to eq(collection.database.name)
          expect(coll).to eq(collection.name)
          expect(sel).to eq(selector)
          expect(opts).to eq(query_opts)
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

