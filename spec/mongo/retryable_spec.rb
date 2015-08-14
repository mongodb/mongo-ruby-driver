require 'spec_helper'

describe Mongo::Retryable do

  let(:klass) do
    Class.new do
      include Mongo::Retryable

      attr_reader :cluster
      attr_reader :operation

      def initialize(operation, cluster)
        @operation = operation
        @cluster = cluster
      end

      def read
        read_with_retry do
          operation.execute
        end
      end

      def write
        write_with_retry do
          operation.execute
        end
      end
    end
  end

  describe '#read_with_retry' do

    let(:operation) do
      double('operation')
    end

    let(:cluster) do
      double('cluster')
    end

    let(:retryable) do
      klass.new(operation, cluster)
    end

    context 'when no exception occurs' do

      before do
        expect(operation).to receive(:execute).and_return(true)
      end

      it 'executes the operation once' do
        expect(retryable.read).to be true
      end
    end

    context 'when a socket error occurs' do

      before do
        expect(operation).to receive(:execute).and_raise(Mongo::Error::SocketError).ordered
        expect(cluster).to receive(:scan!).and_return(true).ordered
        expect(operation).to receive(:execute).and_return(true).ordered
      end

      it 'executes the operation twice' do
        expect(retryable.read).to be true
      end
    end

    context 'when a socket timeout error occurs' do

      before do
        expect(operation).to receive(:execute).and_raise(Mongo::Error::SocketTimeoutError).ordered
        expect(cluster).to receive(:scan!).and_return(true).ordered
        expect(operation).to receive(:execute).and_return(true).ordered
      end

      it 'executes the operation twice' do
        expect(retryable.read).to be true
      end
    end

    context 'when an operation failure occurs' do

      before do
        expect(operation).to receive(:execute).and_raise(Mongo::Error::OperationFailure).ordered
      end

      it 'raises an exception' do
        expect {
          retryable.read
        }.to raise_error(Mongo::Error::OperationFailure)
      end
    end
  end

  describe '#write_with_retry' do

    let(:operation) do
      double('operation')
    end

    let(:cluster) do
      double('cluster')
    end

    let(:retryable) do
      klass.new(operation, cluster)
    end

    context 'when no exception occurs' do

      before do
        expect(operation).to receive(:execute).and_return(true)
      end

      it 'executes the operation once' do
        expect(retryable.write).to be true
      end
    end

    context 'when a not master error occurs' do

      before do
        expect(operation).to receive(:execute).and_raise(Mongo::Error::OperationFailure.new('not master')).ordered
        expect(cluster).to receive(:scan!).and_return(true).ordered
        expect(operation).to receive(:execute).and_return(true).ordered
      end

      it 'executes the operation twice' do
        expect(retryable.write).to be true
      end
    end

    context 'when a normal operation failure occurs' do

      before do
        expect(operation).to receive(:execute).and_raise(Mongo::Error::OperationFailure).ordered
      end

      it 'raises an exception' do
        expect {
          retryable.write
        }.to raise_error(Mongo::Error::OperationFailure)
      end
    end
  end
end
