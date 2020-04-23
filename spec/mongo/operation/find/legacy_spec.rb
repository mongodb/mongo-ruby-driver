require 'spec_helper'

describe Mongo::Operation::Find::Legacy do
  max_server_version '3.4'

  let(:selector) { { foo: 1 } }
  let(:query_options) { {} }
  let(:spec) do
    { :selector  => selector,
      :options      => query_options,
      :db_name   => authorized_collection.database.name,
      :coll_name => authorized_collection.name,
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
          :db_name   => authorized_collection.database.name,
          :coll_name => authorized_collection.name
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

    let(:cluster_single) do
      double('cluster').tap do |c|
        allow(c).to receive(:single?).and_return(true)
      end
    end

    let(:message) do
      authorized_primary.with_connection do |connection|
        query.send(:message, connection)
      end
    end

    it 'applies the correct flags' do
      expect(message.flags).to eq(query_options[:flags])
    end

    context 'when the server is a secondary' do
      let(:description) do
        double('description').tap do |description|
          expect(description).to receive(:mongos?) { false }
          expect(description).to receive(:standalone?) { false }
        end
      end

      let(:connection) do
        double('connection').tap do |conn|
          expect(conn).to receive(:description).at_least(:once).and_return(description)
        end
      end

      let(:secondary_server_single) do
        double('secondary_server').tap do |server|
          expect(server).to receive(:with_connection).and_yield(connection)
          expect(server).to receive(:cluster) { cluster_single }
        end
      end

      let(:message) do
        secondary_server_single.with_connection do |connection|
          query.send(:message, connection)
        end
      end

      before do
        allow(connection).to receive(:server) { secondary_server_single }
      end

      it 'applies the correct flags' do
        expect(message.flags).to eq([ :no_cursor_timeout, :slave_ok ])
      end
    end

    context "when the document contains an 'ok' field" do

      before do
        authorized_collection.insert_one(ok: false)
      end

      after do
        authorized_collection.delete_many
      end

      it 'does not raise an exception' do
        authorized_primary.with_connection do |connection|
          expect(op.execute(connection, client: nil)).to be_a(Mongo::Operation::Find::Legacy::Result)
        end
      end
    end
  end
end

