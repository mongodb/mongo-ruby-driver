require 'spec_helper'

describe Mongo::Server::ConnectionPool::Queue do

  describe '#dequeue' do

    let(:connection) do
      double('connection')
    end

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

        it 'creates a new connecection' do
          expect(queue.dequeue).to eq(connection)
        end
      end
    end

    context 'when waiting for a connection to be enqueued' do

      before do
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

    let(:connection) do
      double('connection')
    end

    let(:queue) do
      described_class.new(:max_pool_size => 1) { connection }
    end

    it 'disconnects all connections in the queue' do
      expect(connection).to receive(:disconnect!)
      queue.disconnect!
    end
  end

  describe '#enqueue' do

    let(:connection) do
      double('connection')
    end

    let(:queue) do
      described_class.new { connection }
    end

    before do
      queue.enqueue(connection)
    end

    it 'adds the connection to the queue' do
      expect(queue.dequeue).to eq(connection)
    end
  end

  describe '#initialize' do

    context 'when a min size is provided' do

      let(:queue) do
        described_class.new(:min_pool_size => 2) { double('connection') }
      end

      it 'creates the queue with the minimum connections' do
        expect(queue.size).to eq(2)
      end

      it 'does not use the same objects in the queue' do
        expect(queue.dequeue).to_not equal(queue.dequeue)
      end
    end

    context 'when no min size is provided' do

      let(:queue) do
        described_class.new { double('connection') }
      end

      it 'creates the queue with the default connections' do
        expect(queue.size).to eq(1)
      end
    end
  end

  describe '#inspect' do

    let(:queue) do
      described_class.new(:min_pool_size => 2) { double('connection') }
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
        described_class.new(:max_pool_size => 3) { double('connection') }
      end

      it 'returns the max size' do
        expect(queue.max_size).to eq(3)
      end
    end

    context 'when no pool size option is provided' do

      let(:queue) do
        described_class.new { double('connection') }
      end

      it 'returns the default size' do
        expect(queue.max_size).to eq(5)
      end
    end
  end

  describe '#wait_timeout' do

    context 'when the wait timeout option is provided' do

      let(:queue) do
        described_class.new(:wait_queue_timeout => 3) { double('connection') }
      end

      it 'returns the wait timeout' do
        expect(queue.wait_timeout).to eq(3)
      end
    end

    context 'when the wait timeout option is not provided' do

      let(:queue) do
        described_class.new { double('connection') }
      end

      it 'returns the default wait timeout' do
        expect(queue.wait_timeout).to eq(1)
      end
    end
  end
end
