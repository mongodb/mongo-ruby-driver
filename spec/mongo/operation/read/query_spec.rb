require 'spec_helper'

describe Mongo::Operation::Read::Query do
  include_context 'operation'

  let(:selector) { {} }
  let(:query_options) { {} }
  let(:spec) do
    { :selector  => selector,
      :options      => query_options,
      :db_name   => db_name,
      :coll_name => coll_name,
      :read => Mongo::ServerSelector.get
    }
  end
  let(:op) { described_class.new(spec) }

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
          :options      => query_options,
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

  describe '#message' do

    let(:query_options) do
      { :flags => [ :no_cursor_timeout ]}
    end

    let(:query) do
      described_class.new(spec)
    end

    let(:message) do
      query.send(:message, secondary_context_slave)
    end

    it 'does not lose flags' do
      expect(message.flags).to eq([ :no_cursor_timeout, :slave_ok ])
    end
  end

  describe '#execute' do

    context 'message' do

      it 'creates a query wire protocol message with correct specs' do
        expect(Mongo::Protocol::Query).to receive(:new) do |db, coll, sel, options|
          expect(db).to eq(db_name)
          expect(coll).to eq(coll_name)
          expect(sel).to eq(selector)
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

    context "when the document contains an 'ok' field" do

      before do
        authorized_collection.insert_one(ok: false)
      end

      after do
        authorized_collection.delete_many
      end

      let(:context) do
        authorized_client.cluster.next_primary.context
      end

      it 'does not raise an exception' do
        expect(op.execute(context)).to be_a(Mongo::Operation::Read::Query::Result)
      end
    end
  end
end

