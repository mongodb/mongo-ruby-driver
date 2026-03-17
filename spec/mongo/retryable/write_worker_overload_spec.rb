# frozen_string_literal: true

require 'lite_spec_helper'

describe Mongo::Retryable::WriteWorker do
  let(:worker_class) do
    Class.new(described_class) do
      public :modern_write_with_retry, :overload_write_retry
    end
  end

  let(:retry_policy) { Mongo::Retryable::RetryPolicy.new(adaptive_retries: false) }

  let(:client) do
    instance_double(Mongo::Client).tap do |c|
      allow(c).to receive(:retry_policy).and_return(retry_policy)
      allow(c).to receive(:options).and_return(retry_writes: true)
      allow(c).to receive(:max_write_retries).and_return(1)
    end
  end

  let(:cluster) { instance_double(Mongo::Cluster) }

  let(:session) do
    instance_double(Mongo::Session).tap do |s|
      allow(s).to receive(:retry_writes?).and_return(true)
      allow(s).to receive(:in_transaction?).and_return(false)
      allow(s).to receive(:starting_transaction?).and_return(false)
      allow(s).to receive(:materialize_if_needed)
      allow(s).to receive(:txn_num).and_return(1)
      allow(s).to receive(:next_txn_num).and_return(2)
    end
  end

  let(:connection) { instance_double(Mongo::Server::Connection) }

  let(:server) do
    instance_double(Mongo::Server).tap do |s|
      allow(s).to receive(:retry_writes?).and_return(true)
      allow(s).to receive(:with_connection).and_yield(connection)
    end
  end

  let(:context) do
    instance_double(Mongo::Operation::Context).tap do |ctx|
      allow(ctx).to receive(:connection_global_id).and_return(nil)
      allow(ctx).to receive(:remaining_timeout_sec).and_return(nil)
      allow(ctx).to receive(:check_timeout!)
      allow(ctx).to receive(:session).and_return(session)
      allow(ctx).to receive(:csot?).and_return(false)
      allow(ctx).to receive(:deadline).and_return(nil)
      allow(ctx).to receive(:dup).and_return(ctx)
      allow(ctx).to receive(:with).and_return(ctx)
    end
  end

  let(:retryable) do
    instance_double(Mongo::Collection).tap do |r|
      allow(r).to receive(:client).and_return(client)
      allow(r).to receive(:cluster).and_return(cluster)
      allow(r).to receive(:select_server).and_return(server)
    end
  end

  let(:worker) { worker_class.new(retryable) }

  def make_overload_error(message = 'overloaded')
    Mongo::Error::OperationFailure.new(
      message,
      nil,
      labels: %w[RetryableWriteError SystemOverloadedError RetryableError]
    )
  end

  def make_retryable_write_error(message = 'not master')
    Mongo::Error::OperationFailure.new(
      message,
      nil,
      labels: %w[RetryableWriteError],
      code: 10_107
    )
  end

  before do
    allow(worker).to receive(:sleep)
  end

  def call_overload_retry(wkr, error: nil, error_count: 1, &block)
    wkr.overload_write_retry(
      error || make_overload_error, session, 2,
      context: context, failed_server: server, error_count: error_count,
      &block
    )
  end

  describe '#modern_write_with_retry' do
    context 'when the operation succeeds on first attempt' do
      it 'records success and returns the result' do
        expect(retry_policy).to receive(:record_success).with(is_retry: false)

        result = worker.modern_write_with_retry(session, server, context) do |_conn, _txn, _ctx|
          :ok
        end

        expect(result).to eq(:ok)
      end
    end

    context 'when an overload error occurs' do
      it 'enters the overload retry loop' do
        call_count = 0
        expect(worker).to receive(:sleep).at_least(:once)

        result = worker.modern_write_with_retry(session, server, context) do |_conn, _txn, _ctx|
          call_count += 1
          raise make_overload_error if call_count == 1

          :recovered
        end

        expect(result).to eq(:recovered)
        expect(call_count).to eq(2)
      end

      it 'does not call ensure_retryable! for overload errors' do
        call_count = 0

        worker.modern_write_with_retry(session, server, context) do |_conn, _txn, _ctx|
          call_count += 1
          raise make_overload_error if call_count == 1

          :ok
        end

        expect(call_count).to eq(2)
      end
    end

    context 'when a non-overload retryable write error occurs' do
      it 'goes through the standard retry_write path' do
        call_count = 0

        result = worker.modern_write_with_retry(session, server, context) do |_conn, _txn, _ctx|
          call_count += 1
          raise make_retryable_write_error if call_count == 1

          :recovered
        end

        expect(result).to eq(:recovered)
        expect(call_count).to eq(2)
      end
    end
  end

  describe '#overload_write_retry' do
    context 'when retry succeeds after backoff' do
      it 'sleeps and retries, returning the result' do
        expect(worker).to receive(:sleep).once
        result = call_overload_retry(worker) { |_c, _t, _x| :write_ok }

        expect(result).to eq(:write_ok)
      end

      it 'records success on retry' do
        expect(retry_policy).to receive(:record_success).with(is_retry: true)
        call_overload_retry(worker) { |_c, _t, _x| :ok }
      end
    end

    context 'with multiple overload errors' do
      it 'retries multiple times with backoff' do
        call_count = 0
        expect(worker).to receive(:sleep).exactly(3).times

        result = call_overload_retry(worker) do |_c, _t, _x|
          call_count += 1
          raise make_overload_error if call_count < 3

          :finally_ok
        end

        expect(result).to eq(:finally_ok)
      end
    end

    context 'when MAX_RETRIES (5) is exceeded' do
      it 'raises the last error' do
        max = Mongo::Retryable::Backpressure::MAX_RETRIES + 1

        expect do
          call_overload_retry(worker, error_count: max) { |_c, _t, _x| :should_not_reach }
        end.to raise_error(Mongo::Error::OperationFailure, /overloaded/)
      end
    end

    context 'when the error count reaches MAX_RETRIES through retries' do
      it 'raises after exhausting retries' do
        call_count = 0

        expect do
          call_overload_retry(worker) do |_c, _t, _x|
            call_count += 1
            raise make_overload_error("overloaded attempt #{call_count}")
          end
        end.to raise_error(Mongo::Error::OperationFailure, /overloaded/)

        expect(call_count).to eq(Mongo::Retryable::Backpressure::MAX_RETRIES)
      end
    end

    context 'when server does not support retryable writes' do
      it 'raises the last error with a note' do
        non_retry_server = instance_double(Mongo::Server)
        allow(non_retry_server).to receive(:retry_writes?).and_return(false)
        allow(retryable).to receive(:select_server).and_return(non_retry_server)

        raised = begin
          call_overload_retry(worker) { |_c, _t, _x| :no }
        rescue Mongo::Error::OperationFailure => e
          e
        end

        expect(raised.notes).to include('did not retry because server does not support retryable writes')
      end
    end

    context 'when server selection fails' do
      it 'raises the original error with a note' do
        allow(retryable).to receive(:select_server)
          .and_raise(Mongo::Error.new('no server'))

        raised = begin
          call_overload_retry(worker) { |_c, _t, _x| :no }
        rescue Mongo::Error::OperationFailure => e
          e
        end

        expect(raised.notes.any? { |n| n.include?('later retry failed') }).to be true
      end
    end

    context 'when a non-overload retryable error occurs during overload loop' do
      it 'records non-overload failure and continues retrying' do
        call_count = 0
        expect(retry_policy).to receive(:record_non_overload_retry_failure).once

        result = call_overload_retry(worker) do |_c, _t, _x|
          call_count += 1
          raise make_retryable_write_error if call_count == 1

          :recovered
        end

        expect(result).to eq(:recovered)
      end
    end
  end

  describe 'record_success on retry_write path' do
    it 'records success after standard retry succeeds' do
      expect(retry_policy).to receive(:record_success).with(is_retry: true)
      call_count = 0

      worker.modern_write_with_retry(session, server, context) do |_conn, _txn, _ctx|
        call_count += 1
        raise make_retryable_write_error if call_count == 1

        :ok
      end
    end
  end

  describe 'adaptive retries (token bucket)' do
    let(:retry_policy) { Mongo::Retryable::RetryPolicy.new(adaptive_retries: true) }

    context 'when the token bucket is exhausted' do
      before { retry_policy.token_bucket.consume(retry_policy.token_bucket.capacity) }

      it 'raises the error instead of retrying' do
        expect do
          call_overload_retry(worker) { |_c, _t, _x| :no }
        end.to raise_error(Mongo::Error::OperationFailure, /overloaded/)
      end
    end

    context 'when there are tokens available' do
      it 'retries and records success' do
        expect(retry_policy).to receive(:record_success).with(is_retry: true)
        call_overload_retry(worker) { |_c, _t, _x| :ok }
      end
    end
  end
end
