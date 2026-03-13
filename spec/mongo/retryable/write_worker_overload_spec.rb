# frozen_string_literal: true

require 'lite_spec_helper'

describe Mongo::Retryable::WriteWorker do
  # Expose private methods for direct testing
  let(:worker_class) do
    Class.new(described_class) do
      public :modern_write_with_retry, :overload_write_retry
    end
  end

  let(:retry_policy) { Mongo::Retryable::RetryPolicy.new(adaptive_retries: false) }

  let(:client) do
    double('client').tap do |c|
      allow(c).to receive(:retry_policy).and_return(retry_policy)
      allow(c).to receive(:options).and_return(retry_writes: true)
      allow(c).to receive(:max_write_retries).and_return(1)
    end
  end

  let(:cluster) { double('cluster') }

  let(:session) do
    double('session').tap do |s|
      allow(s).to receive(:retry_writes?).and_return(true)
      allow(s).to receive(:in_transaction?).and_return(false)
      allow(s).to receive(:materialize_if_needed)
      allow(s).to receive(:txn_num).and_return(1)
      allow(s).to receive(:next_txn_num).and_return(2)
    end
  end

  let(:connection) { double('connection') }

  let(:server) do
    double('server').tap do |s|
      allow(s).to receive(:retry_writes?).and_return(true)
      allow(s).to receive(:with_connection).and_yield(connection)
    end
  end

  let(:context) do
    double('context').tap do |ctx|
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
    double('retryable').tap do |r|
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

        # If ensure_retryable! were called on an overload OperationFailure
        # without RetryableWriteError, it would raise. The fact we get here
        # means overload path was correctly taken.
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
        error = make_overload_error
        call_count = 0

        expect(worker).to receive(:sleep).once

        result = worker.overload_write_retry(
          error, session, 2,
          context: context, failed_server: server, error_count: 1
        ) do |_conn, _txn, _ctx|
          call_count += 1
          :write_ok
        end

        expect(result).to eq(:write_ok)
        expect(call_count).to eq(1)
      end

      it 'records success on retry' do
        error = make_overload_error
        expect(retry_policy).to receive(:record_success).with(is_retry: true)

        worker.overload_write_retry(
          error, session, 2,
          context: context, failed_server: server, error_count: 1
        ) { |_conn, _txn, _ctx| :ok }
      end
    end

    context 'with multiple overload errors' do
      it 'retries multiple times with backoff' do
        error = make_overload_error
        call_count = 0

        expect(worker).to receive(:sleep).exactly(3).times

        result = worker.overload_write_retry(
          error, session, 2,
          context: context, failed_server: server, error_count: 1
        ) do |_conn, _txn, _ctx|
          call_count += 1
          raise make_overload_error if call_count < 3

          :finally_ok
        end

        expect(result).to eq(:finally_ok)
        expect(call_count).to eq(3)
      end
    end

    context 'when MAX_RETRIES (5) is exceeded' do
      it 'raises the last error' do
        error = make_overload_error

        expect do
          worker.overload_write_retry(
            error, session, 2,
            context: context, failed_server: server, error_count: Mongo::Retryable::Backpressure::MAX_RETRIES + 1
          ) { |_conn, _txn, _ctx| :should_not_reach }
        end.to raise_error(Mongo::Error::OperationFailure, /overloaded/)
      end
    end

    context 'when the error count reaches MAX_RETRIES through retries' do
      it 'raises after exhausting retries' do
        call_count = 0

        expect do
          worker.overload_write_retry(
            make_overload_error, session, 2,
            context: context, failed_server: server, error_count: 1
          ) do |_conn, _txn, _ctx|
            call_count += 1
            raise make_overload_error("overloaded attempt #{call_count}")
          end
        end.to raise_error(Mongo::Error::OperationFailure, /overloaded/)

        # MAX_RETRIES is 5; starting at error_count=1, we get retries at
        # counts 1..5, so 5 block invocations before count exceeds limit.
        expect(call_count).to eq(Mongo::Retryable::Backpressure::MAX_RETRIES)
      end
    end

    context 'when server does not support retryable writes' do
      it 'raises the last error with a note' do
        non_retry_server = double('non_retry_server')
        allow(non_retry_server).to receive(:retry_writes?).and_return(false)
        allow(retryable).to receive(:select_server).and_return(non_retry_server)

        error = make_overload_error

        expect do
          worker.overload_write_retry(
            error, session, 2,
            context: context, failed_server: server, error_count: 1
          ) { |_conn, _txn, _ctx| :should_not_reach }
        end.to raise_error(Mongo::Error::OperationFailure) do |e|
          expect(e.notes).to include('did not retry because server does not support retryable writes')
        end
      end
    end

    context 'when server selection fails' do
      it 'raises the original error with a note' do
        allow(retryable).to receive(:select_server)
          .and_raise(Mongo::Error.new('no server'))

        error = make_overload_error

        expect do
          worker.overload_write_retry(
            error, session, 2,
            context: context, failed_server: server, error_count: 1
          ) { |_conn, _txn, _ctx| :should_not_reach }
        end.to raise_error(Mongo::Error::OperationFailure) do |e|
          expect(e.notes.any? { |n| n.include?('later retry failed') }).to be true
        end
      end
    end

    context 'when a non-overload retryable error occurs during overload loop' do
      it 'records non-overload failure and continues retrying' do
        call_count = 0
        expect(retry_policy).to receive(:record_non_overload_retry_failure).once

        result = worker.overload_write_retry(
          make_overload_error, session, 2,
          context: context, failed_server: server, error_count: 1
        ) do |_conn, _txn, _ctx|
          call_count += 1
          if call_count == 1
            raise make_retryable_write_error
          end

          :recovered
        end

        expect(result).to eq(:recovered)
        expect(call_count).to eq(2)
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
      it 'raises the error instead of retrying' do
        # Drain the token bucket
        bucket = retry_policy.token_bucket
        bucket.consume(bucket.capacity)

        error = make_overload_error

        expect do
          worker.overload_write_retry(
            error, session, 2,
            context: context, failed_server: server, error_count: 1
          ) { |_conn, _txn, _ctx| :should_not_reach }
        end.to raise_error(Mongo::Error::OperationFailure, /overloaded/)
      end
    end

    context 'when there are tokens available' do
      it 'retries and records success' do
        error = make_overload_error

        expect(retry_policy).to receive(:record_success).with(is_retry: true)

        result = worker.overload_write_retry(
          error, session, 2,
          context: context, failed_server: server, error_count: 1
        ) { |_conn, _txn, _ctx| :ok }

        expect(result).to eq(:ok)
      end
    end
  end
end
