require 'spec_helper'

describe Mongo::Pool do

  describe '#checkin' do

    let(:server) do
      Mongo::Server.new('127.0.0.1:27017', Mongo::Event::Listeners.new)
    end

    let(:pool) do
      described_class.get(server)
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
      Mongo::Server.new('127.0.0.1:27017', Mongo::Event::Listeners.new)
    end

    let(:pool) do
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
        Thread.new { pool.checkout }.value
      end

      it 'returns a new connection' do
        expect(pool.checkout.address).to eq(server.address)
      end

      it 'does not return the same connection instance' do
        expect(pool.checkout).to_not eql(connection)
      end
    end
  end

  describe '.get' do

    let(:server) do
      Mongo::Server.new('127.0.0.1:27017', Mongo::Event::Listeners.new)
    end

    let(:pool) do
      described_class.get(server)
    end

    it 'returns the pool for the server' do
      expect(pool).to eql(described_class.get(server))
    end
  end
end
