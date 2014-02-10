require 'spec_helper'

describe Mongo::Pool::Queue do

  describe '#dequeue' do

    let(:queue) do
      described_class.new { double('connection') }
    end

    context 'when the queue is empty' do

      it 'raises a timeout error' do
        expect {
          queue.dequeue
        }.to raise_error(Timeout::Error)
      end
    end

    context 'when waiting for a connection to be enqueued' do

      let(:connection) do
        double('connection')
      end

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
end
