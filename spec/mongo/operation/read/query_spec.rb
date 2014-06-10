require 'spec_helper'

describe Mongo::Operation::Read::Query do

  let(:selector) { {} }
  let(:query_opts) { {} }
  let(:db_name) { 'TEST_DB' }
  let(:coll_name) { 'test-coll' }
  let(:spec) do
    { :selector  => selector,
      :opts      => query_opts,
      :db_name   => db_name,
      :coll_name => coll_name
    }
  end
  let(:op) { described_class.new(spec) }
  let(:context) do
    double('context').tap do |cxt|
     allow(cxt).to receive(:with_connection).and_yield(connection)
    end
  end
  let(:connection) do
    double('connection').tap do |conn|
      allow(conn).to receive(:dispatch) { [] }
    end
  end

  describe '#initialize' do

    context 'query spec' do
      it 'sets the query spec' do
        expect(op.spec).to be(spec)
      end
    end
  end

  describe '#==' do

    context 'when two ops have different specs' do
      let(:other_spec) do
        { :selector  => { :a => 1 },
          :opts      => query_opts,
          :db_name   => db_name,
          :coll_name => coll_name
        }
      end
      let(:other) { described_class.new(other_spec) }

      it 'returns false' do
        expect(op).not_to eq(other)
      end
    end
  end

  describe '#execute' do

    context 'message' do

      it 'creates a query wire protocol message with correct specs' do
        expect(Mongo::Protocol::Query).to receive(:new) do |db, coll, sel, opts|
          expect(db).to eq(db_name)
          expect(coll).to eq(coll_name)
          expect(sel).to eq(selector)
          expect(opts).to eq(query_opts)
        end
        op.execute(context)
      end
    end

    context 'connection' do

      it 'dispatches the message on the connection' do
        expect(connection).to receive(:dispatch)
        op.execute(context)
      end
    end
  end
end

