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

      def max_read_retries
        cluster.max_read_retries
      end

      def read_retry_interval
        cluster.read_retry_interval
      end

      def read
        read_with_retry do
          operation.execute
        end
      end

      def write
        write_with_retry(nil, nil) do
          operation.execute
        end
      end
    end
  end

  let(:operation) do
    double('operation')
  end

  let(:cluster) do
    double('cluster', next_primary: server_selector)
  end

  let(:server_selector) do
    double('server_selector', select_server: double('server'))
  end

  let(:retryable) do
    klass.new(operation, cluster)
  end

  describe '#read_with_retry' do

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
        expect(cluster).to receive(:max_read_retries).and_return(1).ordered
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
        expect(cluster).to receive(:max_read_retries).and_return(1).ordered
        expect(cluster).to receive(:scan!).and_return(true).ordered
        expect(operation).to receive(:execute).and_return(true).ordered
      end

      it 'executes the operation twice' do
        expect(retryable.read).to be true
      end
    end

    context 'when an operation failure occurs' do

      context 'when the cluster is not a mongos' do

        before do
          expect(operation).to receive(:execute).and_raise(Mongo::Error::OperationFailure).ordered
          expect(cluster).to receive(:sharded?).and_return(false)
        end

        it 'raises an exception' do
          expect {
            retryable.read
          }.to raise_error(Mongo::Error::OperationFailure)
        end
      end

      context 'when the cluster is a mongos' do

        context 'when the operation failure is not retryable' do

          let(:error) do
            Mongo::Error::OperationFailure.new('not authorized')
          end

          before do
            expect(operation).to receive(:execute).and_raise(error).ordered
            expect(cluster).to receive(:sharded?).and_return(true)
          end

          it 'raises the exception' do
            expect {
              retryable.read
            }.to raise_error(Mongo::Error::OperationFailure)
          end
        end

        context 'when the operation failure is retryable' do

          let(:error) do
            Mongo::Error::OperationFailure.new('not master')
          end

          context 'when the retry succeeds' do

            before do
              expect(operation).to receive(:execute).and_raise(error).ordered
              expect(cluster).to receive(:sharded?).and_return(true)
              expect(cluster).to receive(:max_read_retries).and_return(1).ordered
              expect(cluster).to receive(:read_retry_interval).and_return(0.1).ordered
              expect(operation).to receive(:execute).and_return(true).ordered
            end

            it 'returns the result' do
              expect(retryable.read).to be true
            end
          end

          context 'when the retry fails once and then succeeds' do

            before do
              expect(operation).to receive(:execute).and_raise(error).ordered
              expect(cluster).to receive(:sharded?).and_return(true)
              expect(cluster).to receive(:max_read_retries).and_return(2).ordered
              expect(cluster).to receive(:read_retry_interval).and_return(0.1).ordered
              expect(operation).to receive(:execute).and_raise(error).ordered
              expect(cluster).to receive(:sharded?).and_return(true)
              expect(cluster).to receive(:max_read_retries).and_return(2).ordered
              expect(cluster).to receive(:read_retry_interval).and_return(0.1).ordered
              expect(operation).to receive(:execute).and_return(true).ordered
            end

            it 'returns the result' do
              expect(retryable.read).to be true
            end
          end
        end
      end
    end
  end

  describe '#write_with_retry' do

    context 'when no exception occurs' do

      before do
        expect(operation).to receive(:execute).and_return(true)
      end

      it 'executes the operation once' do
        expect(retryable.write).to be true
      end
    end

    shared_examples 'executes the operation twice' do
      it 'executes the operation twice' do
        expect(retryable.write).to be true
      end
    end

    context 'when a not master error occurs' do

      before do
        expect(operation).to receive(:execute).and_raise(
          Mongo::Error::OperationFailure.new('not master')).ordered
        expect(cluster).to receive(:scan!).and_return(true).ordered
        expect(operation).to receive(:execute).and_return(true).ordered
      end

      it_behaves_like 'executes the operation twice'
    end

    context 'when a node is recovering error occurs' do

      before do
        expect(operation).to receive(:execute).and_raise(
          Mongo::Error::OperationFailure.new('node is recovering')).ordered
        expect(cluster).to receive(:scan!).and_return(true).ordered
        expect(operation).to receive(:execute).and_return(true).ordered
      end

      it_behaves_like 'executes the operation twice'
    end

    context 'when a retryable error occurs with a code' do

      before do
        expect(operation).to receive(:execute).and_raise(
          Mongo::Error::OperationFailure.new('message missing', nil,
            :code => 91, :code_name => 'ShutdownInProgress')).ordered
        expect(cluster).to receive(:scan!).and_return(true).ordered
        expect(operation).to receive(:execute).and_return(true).ordered
      end

      it_behaves_like 'executes the operation twice'
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

    context 'when a socket error occurs' do

      before do
        expect(operation).to receive(:execute).and_raise(
          Mongo::Error::SocketError.new('socket error')).ordered
        expect(cluster).to receive(:scan!).and_return(true).ordered
        expect(operation).to receive(:execute).and_return(true).ordered
      end

      it_behaves_like 'executes the operation twice'
    end

    context 'when a socket timeout occurs' do

      before do
        expect(operation).to receive(:execute).and_raise(
          Mongo::Error::SocketTimeoutError.new('socket timeout')).ordered
        expect(cluster).to receive(:scan!).and_return(true).ordered
        expect(operation).to receive(:execute).and_return(true).ordered
      end

      it_behaves_like 'executes the operation twice'
    end

    context 'when a non-retryable exception occurs' do

      before do
        expect(operation).to receive(:execute).and_raise(
          Mongo::Error::UnsupportedCollation.new('unsupported collation')).ordered
      end

      it 'raises an exception' do
        expect {
          retryable.write
        }.to raise_error(Mongo::Error::UnsupportedCollation)
      end
    end

  end
end
