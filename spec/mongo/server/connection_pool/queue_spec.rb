require 'spec_helper'

describe Mongo::Server::ConnectionPool::Queue do

  def create_connection
    double('connection')
  end

  let(:connection) do
    create_connection
  end

  describe '#dequeue' do

    let(:queue) do
      described_class.new(:max_pool_size => 1) { connection }
    end

    context 'when the queue is empty' do

      context 'when the max size is reached' do

        it 'raises a timeout error' do
          expect {
            queue.dequeue
            queue.dequeue
          }.to raise_error(Timeout::Error)
        end
      end

      context 'when the max size is not reached' do

        it 'creates a new connection' do
          expect(queue.dequeue).to eq(connection)
        end
      end
    end

    context 'when waiting for a connection to be enqueued' do

      before do
        allow(connection).to receive(:record_checkin!).and_return(connection)
        Thread.new do
          sleep(0.5)
          queue.enqueue(connection)
        end.join
      end

      it 'returns the enqueued connection' do
        expect(queue.dequeue).to eq(connection)
      end
    end
  end

  describe '#disconnect!' do

    def create_queue(min_pool_size)
      described_class.new(max_pool_size: 3, min_pool_size: min_pool_size) do
        create_connection
      end.tap do |queue|
        # make queue be of size 2 so that it has enqueued connections
        # when told to disconnect
        c1 = queue.dequeue
        c2 = queue.dequeue
        allow(c1).to receive(:record_checkin!).and_return(c1)
        allow(c2).to receive(:record_checkin!).and_return(c2)
        queue.enqueue(c1)
        queue.enqueue(c2)
        expect(queue.queue.length).to eq(2)
      end
    end

    context 'min size is 0' do
      let(:queue) do
        create_queue(0)
      end

      it 'disconnects all connections in the queue' do
        queue.queue.each do |connection|
          expect(connection).to receive(:disconnect!)
        end
        expect(queue.queue.length).to eq(2)
        queue.disconnect!
        expect(queue.queue.length).to eq(2)
      end
    end
  end

  describe '#enqueue' do

    let(:connection) do
      double('connection').tap do |con|
        allow(con).to receive(:record_checkin!).and_return(con)
      end
    end

    let(:queue) do
      # max pool size set to 2 to allow enqueueing a connection
      # without lint violations
      described_class.new(:max_pool_size => 2) { create_connection }
    end

    it 'adds the connection to the queue' do
      expect(queue.queue.length).to eq(1)
      queue.enqueue(connection)
      expect(queue.queue.length).to eq(2)
      expect(queue.dequeue).to eq(connection)
    end
  end

  describe '#initialize' do

    context 'when a min size is provided' do

      let(:queue) do
        described_class.new(:min_pool_size => 2) { create_connection }
      end

      it 'creates the queue with the minimum connections' do
        expect(queue.pool_size).to eq(2)
        expect(queue.queue_size).to eq(2)
      end

      it 'does not use the same objects in the queue' do
        expect(queue.dequeue).to_not equal(queue.dequeue)
      end
    end

    context 'when min size exceeds default max size' do

      let(:queue) do
        described_class.new(:min_pool_size => 10) { create_connection }
      end

      it 'sets max size to equal provided min size' do
        expect(queue.max_size).to eq(10)
      end
    end

    context 'when no min size is provided' do

      let(:queue) do
        described_class.new { create_connection }
      end

      it 'creates the queue with the number of default connections' do
        expect(queue.pool_size).to eq(1)
        expect(queue.queue_size).to eq(1)
      end
    end
  end

  describe '#inspect' do

    let(:queue) do
      described_class.new(:min_pool_size => 2) { create_connection }
    end

    it 'includes the object id' do
      expect(queue.inspect).to include(queue.object_id.to_s)
    end

    it 'includes the min size' do
      expect(queue.inspect).to include('min_size=2')
    end

    it 'includes the max size' do
      expect(queue.inspect).to include('max_size=5')
    end

    it 'includes the wait timeout' do
      expect(queue.inspect).to include('wait_timeout=1')
    end

    it 'includes the current size' do
      expect(queue.inspect).to include('current_size=2')
    end
  end

  describe '#max_size' do

    context 'when a max pool size option is provided' do

      let(:queue) do
        described_class.new(:max_pool_size => 3) { create_connection }
      end

      it 'returns the max size' do
        expect(queue.max_size).to eq(3)
      end
    end

    context 'when no pool size option is provided' do

      let(:queue) do
        described_class.new { create_connection }
      end

      it 'returns the default size' do
        expect(queue.max_size).to eq(5)
      end
    end
  end

  describe '#wait_timeout' do

    context 'when the wait timeout option is provided' do

      let(:queue) do
        described_class.new(:wait_queue_timeout => 3) { create_connection }
      end

      it 'returns the wait timeout' do
        expect(queue.wait_timeout).to eq(3)
      end
    end

    context 'when the wait timeout option is not provided' do

      let(:queue) do
        described_class.new { create_connection }
      end

      it 'returns the default wait timeout' do
        expect(queue.wait_timeout).to eq(1)
      end
    end
  end

  describe 'close_stale_sockets!!' do

    let(:queue) do
      described_class.new(max_pool_size: 2, max_idle_time: 0.5) do
        double('connection').tap do |con|
          allow(con).to receive(:record_checkin!) do
            allow(con).to receive(:last_checkin).and_return(Time.now)
            con
          end
        end
      end
    end

    let(:connection) do
      queue.dequeue.tap do |con|
        allow(con).to receive(:disconnect!)
      end
    end

    before do
      queue.enqueue(connection)
      expect(connection).to receive(:connect!).and_return(true)
      sleep(0.5)
      queue.close_stale_sockets!
    end

    it 'disconnects and reconnects up to min_size the expired connections' do
      expect(queue.queue_size).to eq(1)
      expect(queue.pool_size).to eq(1)
    end
  end
end
