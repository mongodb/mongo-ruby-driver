require 'spec_helper'

describe Mongo::Pool::Queue do

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
        expect(queue.dequeue(1)).to eq(connection)
      end
    end
  end

  describe '#enqueue' do

    let(:queue) do
      described_class.new { double('connection') }
    end

    let(:connection) do
      double('connection')
    end

    before do
      queue.enqueue(connection)
    end

    it 'adds the connection to the queue' do
      expect(queue.dequeue).to eq(connection)
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
end
