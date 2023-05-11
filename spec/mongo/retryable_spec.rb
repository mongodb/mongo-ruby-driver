# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

class RetryableTestConsumer
  include Mongo::Retryable

  attr_reader :client
  attr_reader :cluster
  attr_reader :operation

  def initialize(operation, cluster, client)
    @operation = operation
    @cluster = cluster
    @client = client
  end

  def max_read_retries
    client.max_read_retries
  end

  def read_retry_interval
    client.read_retry_interval
  end

  def read
    read_with_retry(nil, Mongo::ServerSelector.get(mode: :primary)) do
      operation.execute
    end
  end

  def read_legacy
    read_with_retry do
      operation.execute
    end
  end

  def write
    # This passes a nil session and therefore triggers
    # legacy_write_with_retry code path
    context = Mongo::Operation::Context.new(client: @client, session: session)
    write_with_retry(write_concern, context: context) do
      operation.execute
    end
  end

  def retry_write_allowed_as_configured?
    write_worker.retry_write_allowed?(session, write_concern)
  end
end

class LegacyRetryableTestConsumer < RetryableTestConsumer
  def session
    nil
  end

  def write_concern
    nil
  end
end

class ModernRetryableTestConsumer < LegacyRetryableTestConsumer
  include RSpec::Mocks::ExampleMethods

  def session
    double('session').tap do |session|
      expect(session).to receive(:retry_writes?).and_return(true)
      allow(session).to receive(:materialize_if_needed)

      # mock everything else that is in the way
      i = 1
      allow(session).to receive(:next_txn_num) { i += 1 }
      allow(session).to receive(:in_transaction?).and_return(false)
      allow(session).to receive(:pinned_server)
      allow(session).to receive(:pinned_connection_global_id)
      allow(session).to receive(:starting_transaction?).and_return(false)
      allow(session).to receive(:materialize)
    end
  end

  def write_concern
    nil
  end
end

class RetryableHost
  include Mongo::Retryable

  def retry_write_allowed?(*args)
    write_worker.retry_write_allowed?(*args)
  end
end

describe Mongo::Retryable do

  let(:operation) do
    double('operation')
  end

  let(:connection) do
    double('connection')
  end

  let(:server) do
    double('server').tap do |server|
      allow(server).to receive('with_connection').and_yield(connection)
    end
  end

  let(:max_read_retries) { 1 }
  let(:max_write_retries) { 1 }

  let(:cluster) do
    double('cluster', next_primary: server).tap do |cluster|
      allow(cluster).to receive(:replica_set?).and_return(true)
      allow(cluster).to receive(:addresses).and_return(['x'])
    end
  end

  let(:client) do
    double('client').tap do |client|
      allow(client).to receive(:cluster).and_return(cluster)
      allow(client).to receive(:max_read_retries).and_return(max_read_retries)
      allow(client).to receive(:max_write_retries).and_return(max_write_retries)
    end
  end

  let(:server_selector) do
    double('server_selector', select_server: server)
  end

  let(:retryable) do
    LegacyRetryableTestConsumer.new(operation, cluster, client)
  end

  let(:session) do
    double('session').tap do |session|
      allow(session).to receive(:pinned_connection_global_id).and_return(nil)
      allow(session).to receive(:materialize_if_needed)
    end
  end

  let(:context) do
    Mongo::Operation::Context.new(client: client, session: session)
  end

  before do
    # Retryable reads perform server selection
    allow_any_instance_of(Mongo::ServerSelector::Primary).to receive(:select_server).and_return(server)
  end

  shared_examples_for 'reads with retries' do

    context 'when no exception occurs' do

      before do
        expect(operation).to receive(:execute).and_return(true)
      end

      it 'executes the operation once' do
        expect(read_operation).to be true
      end
    end

    context 'when ending_transaction is true' do
      let(:retryable) { RetryableTestConsumer.new(operation, cluster, client) }

      let(:context) do
        Mongo::Operation::Context.new(client: client, session: nil)
      end

      it 'raises ArgumentError' do
        expect do
          retryable.write_with_retry(nil, ending_transaction: true, context: context) do
            fail 'Expected not to get here'
          end
        end.to raise_error(ArgumentError, 'Cannot end a transaction without a session')
      end
    end

    context 'when a socket error occurs' do

      before do
        expect(retryable).to receive(:select_server).ordered
        expect(operation).to receive(:execute).and_raise(Mongo::Error::SocketError).ordered
        expect(retryable).to receive(:select_server).ordered
        expect(operation).to receive(:execute).and_return(true).ordered
      end

      it 'executes the operation twice' do
        expect(read_operation).to be true
      end
    end

    context 'when a socket timeout error occurs' do

      before do
        expect(retryable).to receive(:select_server).ordered
        expect(operation).to receive(:execute).and_raise(Mongo::Error::SocketTimeoutError).ordered
        expect(retryable).to receive(:select_server).ordered
        expect(operation).to receive(:execute).and_return(true).ordered
      end

      it 'executes the operation twice' do
        expect(read_operation).to be true
      end
    end

    context 'when an operation failure occurs' do

      context 'when the operation failure is not retryable' do

        let(:error) do
          Mongo::Error::OperationFailure.new('not authorized')
        end

        before do
          expect(operation).to receive(:execute).and_raise(error).ordered
        end

        it 'raises the exception' do
          expect {
            read_operation
          }.to raise_error(Mongo::Error::OperationFailure)
        end
      end

      context 'when the operation failure is retryable' do

        let(:error) do
          Mongo::Error::OperationFailure.new('not master')
        end

        context 'when the retry succeeds' do

          before do
            expect(retryable).to receive(:select_server).ordered
            expect(operation).to receive(:execute).and_raise(error).ordered
            expect(client).to receive(:read_retry_interval).and_return(0.1).ordered
            expect(retryable).to receive(:select_server).ordered
            expect(operation).to receive(:execute).and_return(true).ordered
          end

          it 'returns the result' do
            expect(read_operation).to be true
          end
        end

        context 'when the retry fails once and then succeeds' do
          let(:max_read_retries) { 2 }

          before do
            expect(retryable).to receive(:select_server).ordered
            expect(operation).to receive(:execute).and_raise(error.dup).ordered

            expect(client).to receive(:read_retry_interval).and_return(0.1).ordered
            expect(retryable).to receive(:select_server).ordered
            # Since exception is mutated when notes are added to it,
            # we need to ensure each attempt starts with a pristine exception.
            expect(operation).to receive(:execute).and_raise(error.dup).ordered

            expect(client).to receive(:read_retry_interval).and_return(0.1).ordered
            expect(retryable).to receive(:select_server).ordered
            expect(operation).to receive(:execute).and_return(true).ordered
          end

          it 'returns the result' do
            expect(read_operation).to be true
          end
        end
      end
    end
  end

  describe '#read_with_retry' do
    let(:read_operation) do
      retryable.read
    end

    it_behaves_like 'reads with retries'

    context 'zero argument legacy invocation' do

      before do
        allow_any_instance_of(Mongo::ServerSelector::PrimaryPreferred).to receive(:select_server).and_return(server)
      end

      let(:read_operation) do
        retryable.read_legacy
      end

      it_behaves_like 'reads with retries'
    end
  end

  describe '#retry_write_allowed?' do
    let(:retryable) { RetryableHost.new }

    context 'nil session' do
      it 'returns false' do
        expect(retryable.retry_write_allowed?(nil, nil)).to be false
      end
    end

    context 'with session' do
      let(:session) { double('session') }

      context 'retry writes enabled' do
        context 'nil write concern' do
          let(:write_concern) { nil }

          it 'returns true' do
            expect(session).to receive(:retry_writes?).and_return(true)
            expect(retryable.retry_write_allowed?(session, write_concern)).to be true
          end
        end

        context 'hash write concern with w: 0' do
          let(:write_concern) { {w: 0} }

          it 'returns false' do
            expect(session).to receive(:retry_writes?).and_return(true)
            expect(retryable.retry_write_allowed?(session, write_concern)).to be false
          end
        end

        context 'hash write concern with w: :majority' do
          let(:write_concern) { {w: :majority} }

          it 'returns true' do
            expect(session).to receive(:retry_writes?).and_return(true)
            expect(retryable.retry_write_allowed?(session, write_concern)).to be true
          end
        end

        context 'write concern object with w: 0' do
          let(:write_concern) { Mongo::WriteConcern.get(w: 0) }

          it 'returns false' do
            expect(session).to receive(:retry_writes?).and_return(true)
            expect(retryable.retry_write_allowed?(session, write_concern)).to be false
          end
        end

        context 'write concern object with w: :majority' do
          let(:write_concern) { Mongo::WriteConcern.get(w: :majority) }

          it 'returns true' do
            expect(session).to receive(:retry_writes?).and_return(true)
            expect(retryable.retry_write_allowed?(session, write_concern)).to be true
          end
        end
      end

      context 'retry writes not enabled' do
        it 'returns false' do
          expect(session).to receive(:retry_writes?).and_return(false)
          expect(retryable.retry_write_allowed?(session, nil)).to be false
        end
      end
    end
  end

  describe '#write_with_retry - legacy' do

    before do
      # Quick sanity check that the expected code path is being exercised
      expect(retryable.retry_write_allowed_as_configured?).to be false
    end

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

    context 'when an operation failure error occurs with a RetryableWriteError label' do
      let(:error) do
        Mongo::Error::OperationFailure.new(nil, nil, labels: ['RetryableWriteError'])
      end

      before do
        expect(operation).to receive(:execute).and_raise(error).ordered
        expect(cluster).to receive(:scan!).and_return(true).ordered
        expect(operation).to receive(:execute).and_return(true).ordered
      end

      it_behaves_like 'executes the operation twice'
    end

    context 'when an operation failure error occurs without a RetryableWriteError label' do
      before do
        expect(operation).to receive(:execute).and_raise(Mongo::Error::OperationFailure).ordered
      end

      it 'raises an exception' do
        expect {
          retryable.write
        }.to raise_error(Mongo::Error::OperationFailure)
      end
    end

    context 'when a socket error occurs with a RetryableWriteError label' do
      let(:error) do
        error = Mongo::Error::SocketError.new('socket error')
        error.add_label('RetryableWriteError')
        error
      end

      before do
        expect(operation).to receive(:execute).and_raise(error).ordered
      end

      it 'raises an exception' do
        expect {
          retryable.write
        }.to raise_error(Mongo::Error::SocketError)
      end
    end

    context 'when a socket timeout error occurs with a RetryableWriteError label' do
      let(:error) do
        error = Mongo::Error::SocketTimeoutError.new('socket timeout error')
        error.add_label('RetryableWriteError')
        error
      end

      before do
        expect(operation).to receive(:execute).and_raise(error).ordered
      end

      it 'raises an exception' do
        expect {
          retryable.write
        }.to raise_error(Mongo::Error::SocketTimeoutError)
      end
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

  describe '#write_with_retry - modern' do

    let(:retryable) do
      ModernRetryableTestConsumer.new(operation, cluster, client)
    end

    before do
      # Quick sanity check that the expected code path is being exercised
      expect(retryable.retry_write_allowed_as_configured?).to be true

      allow(server).to receive(:retry_writes?).and_return(true)
      allow(cluster).to receive(:scan!)
    end

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

    context 'when an operation failure error occurs with a RetryableWriteError label' do
      let(:error) do
        Mongo::Error::OperationFailure.new(nil, nil, labels: ['RetryableWriteError'])
      end

      before do
        server = cluster.next_primary
        expect(operation).to receive(:execute).and_raise(error).ordered
        expect(operation).to receive(:execute).and_return(true).ordered
      end

      it_behaves_like 'executes the operation twice'
    end

    context 'when an operation failure error occurs without a RetryableWriteError label' do
      before do
        expect(operation).to receive(:execute).and_raise(Mongo::Error::OperationFailure).ordered
      end

      it 'raises an exception' do
        expect {
          retryable.write
        }.to raise_error(Mongo::Error::OperationFailure)
      end
    end

    context 'when a socket error occurs with a RetryableWriteError label' do
      let(:error) do
        error = Mongo::Error::SocketError.new('socket error')
        error.add_label('RetryableWriteError')
        error
      end

      before do
        expect(operation).to receive(:execute).and_raise(error).ordered
        # This is where the server would be marked unknown, but since
        # we are not tracking which server the operation was sent to,
        # we are not able to assert this.
        # There is no explicit cluster scan requested.
        expect(operation).to receive(:execute).and_return(true).ordered
      end

      it_behaves_like 'executes the operation twice'
    end

    context 'when a socket error occurs without a RetryableWriteError label' do
      let(:error) do
        Mongo::Error::SocketError.new('socket error')
      end

      before do
        expect(operation).to receive(:execute).and_raise(error).ordered
      end

      it 'raises an exception' do
        expect {
          retryable.write
        }.to raise_error(Mongo::Error::SocketError)
      end
    end

    context 'when a socket timeout occurs with a RetryableWriteError label' do
      let(:error) do
        error = Mongo::Error::SocketTimeoutError.new('socket timeout error')
        error.add_label('RetryableWriteError')
        error
      end

      before do
        expect(operation).to receive(:execute).and_raise(error).ordered
        # This is where the server would be marked unknown, but since
        # we are not tracking which server the operation was sent to,
        # we are not able to assert this.
        # There is no explicit cluster scan requested (and the operation may
        # end up being sent to the same server it was sent to originally).
        expect(operation).to receive(:execute).and_return(true).ordered
      end

      it_behaves_like 'executes the operation twice'
    end

    context 'when a socket timeout occurs without a RetryableWriteError label' do
      let(:error) do
        Mongo::Error::SocketTimeoutError.new('socket timeout error')
      end

      before do
        expect(operation).to receive(:execute).and_raise(error).ordered
      end

      it 'raises an exception' do
        expect {
          retryable.write
        }.to raise_error(Mongo::Error::SocketTimeoutError)
      end
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

    context 'when an error due to using an unsupported storage engine occurs' do
      before do
        expect(operation).to receive(:execute).and_raise(
          Mongo::Error::OperationFailure.new('message which is not checked',
            nil, code: 20, server_message: 'Transaction numbers are only allowed on...',
        )).ordered
      end

      it 'raises an exception with the correct error message' do
        expect {
          retryable.write
        }.to raise_error(Mongo::Error::OperationFailure, /This MongoDB deployment does not support retryable writes. Please add retryWrites=false to your connection string or use the retry_writes: false Ruby client option/)
      end
    end
  end
end
