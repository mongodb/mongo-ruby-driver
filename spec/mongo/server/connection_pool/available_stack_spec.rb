require 'spec_helper'

describe Mongo::Server::ConnectionPool::AvailableStack do

  def create_connection(generation=1)
    double('connection').tap do |connection|
      allow(connection).to receive(:generation).and_return(generation)
      allow(connection).to receive(:disconnect!)
      allow(connection).to receive(:id).and_return(1)
      allow(connection).to receive(:last_checkin).and_return(nil)
      allow(connection).to receive(:record_checkin!).and_return(connection)
    end
  end

  let(:connection) do
    create_connection
  end

  let(:address) do
    'localhost:99999'
  end

  let(:monitoring) do
    Mongo::Monitoring.new
  end

  describe '#pop' do

    let(:stack) do
      described_class.new(address, monitoring, :max_pool_size => 1) { connection }
    end

    context 'when the stack is empty' do

      context 'when the max size is reached' do

        it 'raises a timeout error' do
          expect {
            stack.pop(Time.now + Mongo::Server::ConnectionPool::WAIT_TIMEOUT)
            stack.pop(Time.now + Mongo::Server::ConnectionPool::WAIT_TIMEOUT)
          }.to raise_error(Mongo::Error::ConnectionCheckoutTimeout)
        end
      end

      context 'when the max size is not reached' do

        it 'creates a new connection' do
          expect(
            stack.pop(Time.now + Mongo::Server::ConnectionPool::WAIT_TIMEOUT)
          ).to eq(connection)
        end
      end
    end

    context 'when waiting for a connection to be pushed' do

      before do
        allow(connection).to receive(:record_checkin!).and_return(connection)
        Thread.new do
          sleep(0.5)
          stack.push(connection)
        end.join
      end

      it 'returns the push connection' do
        expect(stack.pop(Time.now + Mongo::Server::ConnectionPool::WAIT_TIMEOUT)).to eq(connection)
      end
    end
  end

  describe '#disconnect!' do

    def create_stack(min_pool_size)
      described_class.new(address, monitoring, max_pool_size: 3, min_pool_size: min_pool_size) do |generation|
        create_connection(generation)
      end.tap do |stack|
        # make stack be of size 2 so that it has pushed connections
        # when told to disconnect
        c1 = stack.pop(Time.now + Mongo::Server::ConnectionPool::WAIT_TIMEOUT)
        c2 = stack.pop(Time.now + Mongo::Server::ConnectionPool::WAIT_TIMEOUT)
        allow(c1).to receive(:record_checkin!).and_return(c1)
        allow(c2).to receive(:record_checkin!).and_return(c2)
        stack.push(c1)
        stack.push(c2)
        expect(stack.stack_size).to eq(2)
        expect(stack.pool_size).to eq(2)
      end
    end

    context 'min size is 0' do
      let(:stack) do
        create_stack(0)
      end

      it 'disconnects and removes all connections in the stack' do
        stack.connections.each do |connection|
          expect(connection).to receive(:disconnect!)
        end
        expect(stack.stack_size).to eq(2)
        expect(stack.pool_size).to eq(2)
        stack.close!
        expect(stack.stack_size).to eq(0)
        expect(stack.pool_size).to eq(0)
      end
    end

    context 'min size is not 0' do
      let(:stack) do
        create_stack(1)
      end

      it 'disconnects all connections in the stack and increments the generation but does not create new connections' do
        expect(stack.stack_size).to eq(2)
        expect(stack.pool_size).to eq(2)

        stack.close!

        expect(stack.stack_size).to eq(0)
        expect(stack.pool_size).to eq(0)

        new_connection = stack.pop(Time.now + Mongo::Server::ConnectionPool::WAIT_TIMEOUT)
        expect(new_connection).not_to eq(connection)
        expect(new_connection.generation).to eq(2)
      end
    end
  end

  describe '#push' do

    let(:connection) do
      stack.pop(Time.now + Mongo::Server::ConnectionPool::WAIT_TIMEOUT).tap do |connection|
        allow(connection).to receive(:generation).and_return(1)
        allow(connection).to receive(:record_checkin!).and_return(connection)
      end
    end

    let(:stack) do
      # max pool size set to 2 to allow pushing a connection
      # without lint violations
      described_class.new(address, monitoring, :max_pool_size => 2) { create_connection }
    end

    context 'connection of the same generation as stack' do
      before do
        expect(stack.generation).to eq(connection.generation)
      end

      it 'adds the connection to the stack' do
        # connection is checked out
        expect(stack.stack_size).to eq(0)
        expect(stack.pool_size).to eq(1)
        stack.push(connection)
        # now connection is in the stack
        expect(stack.stack_size).to eq(1)
        expect(stack.pool_size).to eq(1)
        expect(stack.pop(Time.now + Mongo::Server::ConnectionPool::WAIT_TIMEOUT)).to eq(connection)
      end
    end

    shared_examples 'does not add connection to stack' do
      before do
        expect(stack.generation).not_to eq(connection.generation)
      end

      it 'disconnects connection and does not add connection to stack' do
        # connection was checked out
        expect(stack.stack_size).to eq(0)
        expect(stack.pool_size).to eq(1)
        expect(connection).to receive(:disconnect!)
        stack.push(connection)
        expect(stack.stack_size).to eq(0)
        expect(stack.pool_size).to eq(0)
        expect(
          stack.pop(Time.now + Mongo::Server::ConnectionPool::WAIT_TIMEOUT)
        ).not_to eq(connection)
      end
    end

    context 'connection of earlier generation than stack' do
      let(:connection) do
        stack.pop(Time.now + Mongo::Server::ConnectionPool::WAIT_TIMEOUT).tap do |connection|
          allow(connection).to receive(:generation).and_return(0)
          allow(connection).to receive(:record_checkin!).and_return(connection)
        end
      end

      it_behaves_like 'does not add connection to stack'
    end

    context 'connection of later generation than stack' do
      let(:connection) do
        stack.pop(Time.now + Mongo::Server::ConnectionPool::WAIT_TIMEOUT).tap do |connection|
          allow(connection).to receive(:generation).and_return(7)
          allow(connection).to receive(:record_checkin!).and_return(connection)
        end
      end

      it_behaves_like 'does not add connection to stack'
    end
  end

  describe '#initialize' do

    context 'when a min size is provided' do

      let(:stack) do
        described_class.new(address, monitoring, :min_pool_size => 2) { create_connection }
      end

      it 'creates the stack with the minimum connections' do
        expect(stack.pool_size).to eq(2)
        expect(stack.stack_size).to eq(2)
      end

      it 'does not use the same objects in the stack' do
        expect(
          stack.pop(Time.now + Mongo::Server::ConnectionPool::WAIT_TIMEOUT)
        ).to_not equal(stack.pop(Time.now + Mongo::Server::ConnectionPool::WAIT_TIMEOUT))
      end
    end

    context 'when min size exceeds default max size' do

      let(:stack) do
        described_class.new(address, monitoring, :min_pool_size => 10) { create_connection }
      end

      it 'sets max size to equal provided min size' do
        expect(stack.max_size).to eq(10)
      end
    end

    context 'when no min size is provided' do

      let(:stack) do
        described_class.new(address, monitoring) { create_connection }
      end

      it 'creates the stack with the number of default connections' do
        expect(stack.pool_size).to eq(0)
        expect(stack.stack_size).to eq(0)
      end
    end
  end

  describe '#inspect' do

    let(:stack) do
      described_class.new(address, monitoring, :min_pool_size => 2) { create_connection }
    end

    it 'includes the object id' do
      expect(stack.inspect).to include(stack.object_id.to_s)
    end

    it 'includes the min size' do
      expect(stack.inspect).to include('min_size=2')
    end

    it 'includes the max size' do
      expect(stack.inspect).to include('max_size=5')
    end

    it 'includes the current size' do
      expect(stack.inspect).to include('current_size=2')
    end
  end

  describe '#max_size' do

    context 'when a max pool size option is provided' do

      let(:stack) do
        described_class.new(address, monitoring, :max_pool_size => 3) { create_connection }
      end

      it 'returns the max size' do
        expect(stack.max_size).to eq(3)
      end
    end

    context 'when no pool size option is provided' do

      let(:stack) do
        described_class.new(address, monitoring) { create_connection }
      end

      it 'returns the default size' do
        expect(stack.max_size).to eq(5)
      end
    end
  end

  describe 'close_stale_sockets!' do

    let(:stack) do
      described_class.new(address, monitoring, max_pool_size: 2, max_idle_time: 0.5) do
        double('connection').tap do |con|
          expect(con).to receive(:generation).and_return(2)
          expect(con).to receive(:disconnect!).and_return(true)
          allow(con).to receive(:id).and_return(1)
          allow(con).to receive(:record_checkin!) do
            allow(con).to receive(:last_checkin).and_return(Time.now)
            con
          end
        end
      end
    end

    let(:connection) do
      stack.pop(Time.now + Mongo::Server::ConnectionPool::WAIT_TIMEOUT).tap do |con|
        allow(con).to receive(:disconnect!)
      end
    end

    before do
      stack.push(connection)
      expect(connection).not_to receive(:connect!)
      sleep(0.5)
      stack.close_stale_sockets!
    end

    it 'disconnects and does not reconnect the sockets' do
      expect(stack.stack_size).to eq(0)
      expect(stack.pool_size).to eq(0)
    end
  end
end
