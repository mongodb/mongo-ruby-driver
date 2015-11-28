require 'spec_helper'

describe Mongo::Server::ConnectionPool do

  let(:options) do
    TEST_OPTIONS.merge(max_pool_size: 2)
  end

  let(:address) do
    Mongo::Address.new('127.0.0.1:27017')
  end

  let(:monitoring) do
    Mongo::Monitoring.new
  end

  let(:listeners) do
    Mongo::Event::Listeners.new
  end

  let(:cluster) do
    double('cluster')
  end

  describe '#checkin' do

    let(:server) do
      Mongo::Server.new(address, cluster, monitoring, listeners, options)
    end

    let!(:pool) do
      described_class.get(server)
    end

    after do
      expect(cluster).to receive(:pool).with(server).and_return(pool)
      server.disconnect!
    end

    context 'when a connection is checked out on the thread' do

      let!(:connection) do
        pool.checkout
      end

      before do
        pool.checkin(connection)
      end

      let(:queue) do
        pool.send(:queue).queue
      end

      it 'returns the connection to the queue' do
        expect(queue.size).to eq(1)
      end
    end
  end

  describe '#checkout' do

    let(:server) do
      Mongo::Server.new(address, cluster, monitoring, listeners, options)
    end

    let!(:pool) do
      described_class.get(server)
    end

    context 'when no connection is checked out on the same thread' do

      let!(:connection) do
        pool.checkout
      end

      it 'returns a new connection' do
        expect(connection.address).to eq(server.address)
      end
    end

    context 'when a connection is checked out on the same thread' do

      before do
        pool.checkout
      end

      it 'returns the threads connection' do
        expect(pool.checkout.address).to eq(server.address)
      end
    end

    context 'when a connection is checked out on a different thread' do

      let!(:connection) do
        Thread.new { pool.checkout }.join
      end

      it 'returns a new connection' do
        expect(pool.checkout.address).to eq(server.address)
      end

      it 'does not return the same connection instance' do
        expect(pool.checkout).to_not eql(connection)
      end
    end
  end

  describe '#disconnect!' do

    let(:server) do
      Mongo::Server.new(address, cluster, monitoring, listeners, options)
    end

    let!(:pool) do
      described_class.get(server)
    end

    it 'disconnects the queue' do
      expect(cluster).to receive(:pool).with(server).and_return(pool)
      expect(pool.send(:queue)).to receive(:disconnect!).once.and_call_original
      server.disconnect!
    end
  end

  describe '.get' do

    let(:server) do
      Mongo::Server.new(address, cluster, monitoring, listeners, options)
    end

    let!(:pool) do
      described_class.get(server)
    end

    after do
      expect(cluster).to receive(:pool).with(server).and_return(pool)
      server.disconnect!
    end

    it 'returns the pool for the server' do
      expect(pool).to_not be_nil
    end
  end

  describe '#inspect' do

    let(:server) do
      Mongo::Server.new(address, cluster, monitoring, listeners, options)
    end

    let!(:pool) do
      described_class.get(server)
    end

    after do
      expect(cluster).to receive(:pool).with(server).and_return(pool)
      server.disconnect!
    end

    it 'includes the object id' do
      expect(pool.inspect).to include(pool.object_id.to_s)
    end

    it 'includes the queue inspection' do
      expect(pool.inspect).to include(pool.__send__(:queue).inspect)
    end
  end

  describe '#with_connection' do

    let(:server) do
      Mongo::Server.new(address, cluster, monitoring, listeners, options)
    end

    let!(:pool) do
      described_class.get(server)
    end

    context 'when a connection cannot be checked out' do

      before do
        allow(pool).to receive(:checkout).and_return(nil)
        pool.with_connection { |c| c }
      end

      let(:queue) do
        pool.send(:queue).queue
      end

      it 'does not add the connection to the pool' do
        expect(queue.size).to eq(1)
      end
    end
  end
end
