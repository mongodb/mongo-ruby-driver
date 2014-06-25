require 'spec_helper'

describe Mongo::Operation::Aggregate do
  include_context 'operation'

  let(:selector) do
    { :pipeline => [] }
  end
  let(:spec) do
    { :selector => selector,
      :opts => {}
    }
  end
  let(:op) { described_class.new(collection, spec) }


  describe '#initialize' do

    context 'spec' do

      it 'sets the spec' do
        expect(op.spec).to be(spec)
      end
    end

    context 'collection' do

      it 'sets the collection' do
        expect(op.collection).to be(collection)
      end
    end
  end

  describe '#==' do

    context ' when two ops have different specs' do
      let(:other_selector) do 
        { :pipeline => [{ '$out' => 'other-test-coll' }] }
      end

      let(:other_spec) do
        { :selector => other_selector,
          :opts => opts
        }
      end
      let(:other) { described_class.new(collection, other_spec) }

      it 'returns false' do
        expect(op).not_to eq(other)
      end
    end

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

  describe '#execute' do

    context 'message' do

      it 'creates a query wire protocol message with correct specs' do
        allow_any_instance_of(Mongo::ServerPreference::Primary).to receive(:server) do
          primary_server
        end

        expect(Mongo::Protocol::Query).to receive(:new) do |db, coll, sel, options|
          expect(db).to eq(collection.database.name)
          expect(coll).to eq(Mongo::Operation::COMMAND_COLLECTION_NAME)
          expect(sel).to eq(selector.merge('aggregate' => collection.name))
          expect(options).to eq(opts)
        end
        op.execute(primary_context)
      end
    end

    context 'connection' do

      it 'dispatches the message on the connection' do
        allow_any_instance_of(Mongo::ServerPreference::Primary).to receive(:server) do
          primary_server
        end

        expect(connection).to receive(:dispatch)
        op.execute(primary_context)
      end
    end

    context 'rerouting' do

      context 'when out is specified and server is a secondary' do
        let(:selector) do
          { :pipeline => [{ '$out' => 'test_coll' }] }
        end

        it 'reroutes the operation to the primary' do
          allow_any_instance_of(Mongo::ServerPreference::Primary).to receive(:server) do
            primary_server
          end
          expect(primary_context).to receive(:with_connection)
          op.execute(secondary_context)
        end
      end

      context 'when out is specified and server is a primary' do
        let(:selector) do
          { :pipeline => [{ '$out' => 'test_coll' }] }
        end

        it 'sends the operation to the primary' do
          allow_any_instance_of(Mongo::ServerPreference::Primary).to receive(:server) do
            primary_server
          end
          expect(primary_context).to receive(:with_connection)
          op.execute(primary_context)
        end
      end
    end
  end
end
