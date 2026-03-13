# frozen_string_literal: true

require 'spec_helper'

describe Mongo::Session do
  describe '#with_transaction overload retries' do
    let(:retry_policy) do
      Mongo::Retryable::RetryPolicy.new(adaptive_retries: false)
    end

    let(:client) do
      instance_double(Mongo::Client).tap do |c|
        allow(c).to receive(:retry_policy).and_return(retry_policy)
        allow(c).to receive(:timeout_ms).and_return(nil)
      end
    end

    let(:session) do
      # Build a minimal session-like object that has the methods we need.
      # We use allocate + manual setup to avoid needing a real server connection.
      sess = described_class.allocate

      # Set the instance variables that with_transaction depends on.
      sess.instance_variable_set(:@client, client)
      sess.instance_variable_set(:@options, {})
      sess.instance_variable_set(:@state, Mongo::Session::NO_TRANSACTION_STATE)
      sess.instance_variable_set(:@lock, Mutex.new)

      allow(sess).to receive(:check_transactions_supported!).and_return(true)
      allow(sess).to receive(:check_if_ended!)
      allow(sess).to receive(:log_warn)

      sess
    end

    def make_transient_overload_error
      error = Mongo::Error::OperationFailure.new('overloaded')
      error.add_label('TransientTransactionError')
      error.add_label('SystemOverloadedError')
      error
    end

    def make_transient_error
      error = Mongo::Error::OperationFailure.new('transient')
      error.add_label('TransientTransactionError')
      error
    end

    def make_commit_overload_error
      error = Mongo::Error::OperationFailure.new('commit overloaded')
      error.add_label('UnknownTransactionCommitResult')
      error.add_label('SystemOverloadedError')
      error
    end

    def make_commit_transient_overload_error
      error = Mongo::Error::OperationFailure.new('commit transient overloaded')
      error.add_label('TransientTransactionError')
      error.add_label('SystemOverloadedError')
      error
    end

    before do
      # Stub start_transaction to just set the state
      allow(session).to receive(:start_transaction) do |*_args|
        session.instance_variable_set(:@state, Mongo::Session::STARTING_TRANSACTION_STATE)
      end

      # Stub abort_transaction
      allow(session).to receive(:abort_transaction) do
        session.instance_variable_set(:@state, Mongo::Session::TRANSACTION_ABORTED_STATE)
      end

      # Stub commit_transaction by default (tests override as needed)
      allow(session).to receive(:commit_transaction) do
        session.instance_variable_set(:@state, Mongo::Session::TRANSACTION_COMMITTED_STATE)
      end

      # Use deterministic jitter for backoff_delay
      allow(retry_policy).to receive(:backoff_delay).and_wrap_original do |method, attempt, **_kwargs|
        method.call(attempt, jitter: 1.0)
      end

      # Spy on record_non_overload_retry_failure
      allow(retry_policy).to receive(:record_non_overload_retry_failure).and_call_original

      # Make sleep a no-op to speed up tests
      allow(session).to receive(:sleep)
    end

    context 'when callback raises TransientTransactionError with SystemOverloadedError' do
      it 'uses the new overload backoff' do
        call_count = 0
        error = make_transient_overload_error

        session.with_transaction do
          call_count += 1
          raise error if call_count == 1
        end

        # With jitter=1.0, attempt=1: min(10, 0.1 * 2^0) = 0.1
        expect(session).to have_received(:sleep).with(0.1).once
      end
    end

    context 'when callback raises TransientTransactionError without SystemOverloadedError' do
      it 'uses the existing backoff' do
        call_count = 0
        error = make_transient_error

        session.with_transaction do
          call_count += 1
          raise error if call_count == 1
        end

        # Existing backoff: backoff_seconds_for_retry uses 5ms base, 1.5^n multiplier
        # For attempt 1: 0.005 * 1.5^0 = 0.005
        expect(session).to have_received(:sleep).once
        expect(session).not_to have_received(:sleep).with(0.1)
      end
    end

    context 'when overload errors exceed MAX_RETRIES' do
      it 'raises the error after MAX_RETRIES' do
        max_retries = Mongo::Retryable::Backpressure::MAX_RETRIES
        call_count = 0
        error = make_transient_overload_error

        expect do
          session.with_transaction do
            call_count += 1
            raise error
          end
        end.to raise_error(Mongo::Error::OperationFailure, 'overloaded')

        # should_retry_overload? returns false when attempt > MAX_RETRIES
        # The callback runs MAX_RETRIES+1 times (first attempt + MAX_RETRIES retries),
        # then on retry MAX_RETRIES+1 the should_retry_overload? check fails.
        expect(call_count).to eq(max_retries + 1)
      end
    end

    context 'when overload is encountered then non-overload transient follows' do
      it 'uses overload backoff for the subsequent non-overload error' do
        overload_error = make_transient_overload_error
        transient_error = make_transient_error
        call_count = 0

        session.with_transaction do
          call_count += 1
          case call_count
          when 1
            raise overload_error
          when 2
            raise transient_error
          end
        end

        expect(retry_policy).to have_received(:backoff_delay).twice
        expect(retry_policy).to have_received(:record_non_overload_retry_failure).once
      end
    end

    context 'when commit raises UnknownTransactionCommitResult with SystemOverloadedError' do
      it 'applies overload backoff during commit retry' do
        commit_error = make_commit_overload_error
        commit_count = 0

        allow(session).to receive(:commit_transaction) do
          commit_count += 1
          raise commit_error if commit_count == 1

          session.instance_variable_set(:@state, Mongo::Session::TRANSACTION_COMMITTED_STATE)
        end

        session.with_transaction do
          session.instance_variable_set(:@state, Mongo::Session::TRANSACTION_IN_PROGRESS_STATE)
        end

        # Overload backoff should be used for the commit retry
        expect(retry_policy).to have_received(:backoff_delay).with(1).once
        expect(session).to have_received(:sleep).with(0.1).once
      end
    end

    context 'when commit raises TransientTransactionError with SystemOverloadedError' do
      it 'tracks overload state for the next transaction attempt' do
        commit_error = make_commit_transient_overload_error
        call_count = 0

        allow(session).to receive(:commit_transaction) do
          if call_count == 1
            call_count += 1
            raise commit_error
          end
          session.instance_variable_set(:@state, Mongo::Session::TRANSACTION_COMMITTED_STATE)
        end

        session.with_transaction do
          call_count += 1
          session.instance_variable_set(:@state, Mongo::Session::TRANSACTION_IN_PROGRESS_STATE)
        end

        # The overload backoff should be used on the retry loop iteration
        expect(retry_policy).to have_received(:backoff_delay).once
      end
    end

    context 'when commit overload errors exceed MAX_RETRIES' do
      it 'raises after MAX_RETRIES' do
        max_retries = Mongo::Retryable::Backpressure::MAX_RETRIES
        commit_error = make_commit_overload_error

        allow(session).to receive(:commit_transaction).and_raise(commit_error)

        expect do
          session.with_transaction do
            session.instance_variable_set(:@state, Mongo::Session::TRANSACTION_IN_PROGRESS_STATE)
          end
        end.to raise_error(Mongo::Error::OperationFailure, 'commit overloaded')

        # should_retry_overload? returns false when attempt > MAX_RETRIES
        expect(retry_policy).to have_received(:backoff_delay).exactly(max_retries + 1).times
      end
    end
  end
end
