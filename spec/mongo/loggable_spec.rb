require 'spec_helper'

describe Mongo::Loggable do

  let(:operation) do
    Class.new do
      def log_message
        'test'
      end
    end.new
  end

  describe '#log' do

    let(:loggable) do
      Class.new do
        include Mongo::Loggable
      end.new
    end

    let(:operation) do
      double('operation')
    end

    before do
      Mongo::Logger.level = Logger::DEBUG
      expect(operation).to receive(:log_message).and_return('test')
      expect(Mongo::Logger).to receive(:log).with(:debug, 'MONGO', 'test', anything())
    end

    context 'when a block is provided' do

      context 'when an exception occurs' do

        it 'logs the message' do
          expect {
            loggable.log(:debug, 'MONGO', [ operation ]) do
              raise RuntimeError
            end
          }.to raise_error(RuntimeError)
        end
      end

      context 'when no exception occurs' do

        it 'executes the block and logs the message' do
          expect(
            loggable.log(:debug, 'MONGO', [ operation ]) do
              'testing'
            end
          ).to eq('testing')
        end
      end
    end

    context 'when no block is provided' do

      it 'logs the message' do
        expect(loggable.log(:debug, 'MONGO', [ operation ])).to be_nil
      end
    end
  end
end
