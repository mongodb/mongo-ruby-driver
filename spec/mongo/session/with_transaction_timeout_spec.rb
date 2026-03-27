# frozen_string_literal: true

require 'spec_helper'

# Prose tests for the "Retry Timeout is Enforced" section of the
# transactions-convenient-api spec README.
#
# specifications/source/transactions-convenient-api/tests/README.md
#
# Three sub-cases must be covered:
#   1. Callback raises TransientTransactionError and timeout is exceeded.
#   2. Commit raises UnknownTransactionCommitResult and timeout is exceeded.
#   3. Commit raises TransientTransactionError and timeout is exceeded.
#
# Note 1 from spec: "The error SHOULD be propagated as a timeout error if
# the language allows to expose the underlying error as a cause of a timeout
# error." Ruby supports this via Exception#cause.
describe 'Mongo::Session#with_transaction Retry Timeout is Enforced' do
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
    sess = Mongo::Session.allocate
    sess.instance_variable_set(:@client, client)
    sess.instance_variable_set(:@options, {})
    sess.instance_variable_set(:@state, Mongo::Session::NO_TRANSACTION_STATE)
    sess.instance_variable_set(:@lock, Mutex.new)
    allow(sess).to receive(:check_transactions_supported!).and_return(true)
    allow(sess).to receive(:check_if_ended!)
    allow(sess).to receive(:log_warn)
    allow(sess).to receive(:session_id).and_return(BSON::Document.new('id' => 'test'))
    sess
  end

  before do
    allow(session).to receive(:start_transaction) do |*_args|
      session.instance_variable_set(:@state, Mongo::Session::STARTING_TRANSACTION_STATE)
    end

    allow(session).to receive(:abort_transaction) do
      session.instance_variable_set(:@state, Mongo::Session::TRANSACTION_ABORTED_STATE)
    end

    allow(session).to receive(:commit_transaction) do
      session.instance_variable_set(:@state, Mongo::Session::TRANSACTION_COMMITTED_STATE)
    end

    allow(session).to receive(:sleep)
  end

  # Stubs Mongo::Utils.monotonic_time to return a fixed "present" time for the
  # first `initial_calls` invocations, then a time far in the future for all
  # remaining calls.  Combined with timeout_ms: 1 (deadline ≈ present + 0.001 s)
  # this makes every deadline check after the Nth call return "expired".
  def with_expired_deadline_after(initial_calls:, &block)
    call_count = 0
    allow(Mongo::Utils).to receive(:monotonic_time) do
      call_count += 1
      call_count <= initial_calls ? 100.0 : 200.0
    end
    block.call
  end

  def make_transient_error
    error = Mongo::Error::OperationFailure.new('transient')
    error.add_label('TransientTransactionError')
    error
  end

  def make_commit_unknown_error
    error = Mongo::Error::OperationFailure.new('commit unknown')
    error.add_label('UnknownTransactionCommitResult')
    error
  end

  def make_commit_transient_error
    error = Mongo::Error::OperationFailure.new('commit transient')
    error.add_label('TransientTransactionError')
    error
  end

  def make_transient_overload_error
    error = Mongo::Error::OperationFailure.new('transient overload')
    error.add_label('TransientTransactionError')
    error.add_label('SystemOverloadedError')
    error
  end

  def make_commit_overload_error
    error = Mongo::Error::OperationFailure.new('commit overload')
    error.add_label('UnknownTransactionCommitResult')
    error.add_label('SystemOverloadedError')
    error
  end

  # Sub-case 1: callback raises TransientTransactionError + timeout exceeded
  context 'when callback raises TransientTransactionError and retry timeout is exceeded' do
    it 'propagates the error as TimeoutError with the transient error as cause' do
      transient_error = make_transient_error
      call_count = 0

      # Call 1 → deadline setup (returns 100.0, deadline = 100.001).
      # Calls 2+ → 200.0, so deadline_expired? is true for subsequent checks.
      with_expired_deadline_after(initial_calls: 1) do
        expect do
          session.with_transaction(timeout_ms: 1) do
            call_count += 1
            raise transient_error
          end
        end.to raise_error(Mongo::Error::TimeoutError) do |err|
          expect(err.cause).to eq(transient_error)
        end
      end

      expect(call_count).to eq(1)
    end
  end

  # Sub-case 2: commit raises UnknownTransactionCommitResult + timeout exceeded
  context 'when commit raises UnknownTransactionCommitResult and retry timeout is exceeded' do
    it 'propagates the error as TimeoutError with the commit error as cause' do
      commit_error = make_commit_unknown_error

      allow(session).to receive(:commit_transaction) do
        raise commit_error
      end

      # Call 1 → deadline setup (100.0, deadline = 100.001).
      # Call 2 → pre-commit CSOT check at line 540 (100.0, not expired → skip).
      # Calls 3+ → 200.0, expired → deadline_expired? true inside commit rescue.
      with_expired_deadline_after(initial_calls: 2) do
        expect do
          session.with_transaction(timeout_ms: 1) do
            session.instance_variable_set(:@state, Mongo::Session::TRANSACTION_IN_PROGRESS_STATE)
          end
        end.to raise_error(Mongo::Error::TimeoutError) do |err|
          expect(err.cause).to eq(commit_error)
        end
      end
    end
  end

  # Sub-case 3: commit raises TransientTransactionError + timeout exceeded
  context 'when commit raises TransientTransactionError and retry timeout is exceeded' do
    it 'propagates the error as TimeoutError with the commit error as cause' do
      commit_error = make_commit_transient_error

      allow(session).to receive(:commit_transaction) do
        raise commit_error
      end

      # Same time-control logic as sub-case 2.
      with_expired_deadline_after(initial_calls: 2) do
        expect do
          session.with_transaction(timeout_ms: 1) do
            session.instance_variable_set(:@state, Mongo::Session::TRANSACTION_IN_PROGRESS_STATE)
          end
        end.to raise_error(Mongo::Error::TimeoutError) do |err|
          expect(err.cause).to eq(commit_error)
        end
      end
    end
  end
end

# Tests for the "backoff would exceed deadline" check that fires at the top of
# the retry loop (before sleeping).  Two bugs exist here:
#   1. In CSOT mode the raised TimeoutError has no .cause (last_error is lost).
#   2. In non-CSOT mode a TimeoutError is raised instead of last_error.
# The same two bugs apply to the commit-overload backoff path.
describe 'Mongo::Session#with_transaction Backoff Deadline is Enforced' do
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
    sess = Mongo::Session.allocate
    sess.instance_variable_set(:@client, client)
    sess.instance_variable_set(:@options, {})
    sess.instance_variable_set(:@state, Mongo::Session::NO_TRANSACTION_STATE)
    sess.instance_variable_set(:@lock, Mutex.new)
    allow(sess).to receive(:check_transactions_supported!).and_return(true)
    allow(sess).to receive(:check_if_ended!)
    allow(sess).to receive(:log_warn)
    allow(sess).to receive(:session_id).and_return(BSON::Document.new('id' => 'test'))
    sess
  end

  before do
    allow(session).to receive(:start_transaction) do |*_args|
      session.instance_variable_set(:@state, Mongo::Session::STARTING_TRANSACTION_STATE)
    end

    allow(session).to receive(:abort_transaction) do
      session.instance_variable_set(:@state, Mongo::Session::TRANSACTION_ABORTED_STATE)
    end

    allow(session).to receive(:commit_transaction) do
      session.instance_variable_set(:@state, Mongo::Session::TRANSACTION_COMMITTED_STATE)
    end

    allow(session).to receive(:sleep)

    # Fix jitter at 1.0 so backoff values are deterministic.
    allow(Random).to receive(:rand).and_return(1.0)
    allow(retry_policy).to receive(:backoff_delay).and_wrap_original do |m, attempt, **_|
      m.call(attempt, jitter: 1.0)
    end
  end

  # CSOT time control:
  #   monotonic_time always returns 100.0
  #   timeout_ms: 1  →  deadline = 100.001
  #   regular backoff attempt 1 = 0.005 s  →  100.005 >= 100.001  →  exceeds deadline
  #   overload backoff attempt 1 = 0.1 s   →  100.1   >= 100.001  →  exceeds deadline
  #   deadline_expired? at line 514 (after error) = 100.0 >= 100.001 → false (skip)
  def with_csot_backoff_time_control
    allow(Mongo::Utils).to receive(:monotonic_time).and_return(100.0)
    yield
  end

  # non-CSOT time control:
  #   1st monotonic_time call (deadline setup) → 100.0  ⟹  deadline = 220.0
  #   subsequent calls → 219.996
  #   deadline_expired? = 219.996 >= 220.0 → false (won't fire before backoff check)
  #   regular backoff:  219.996 + 0.005 = 220.001 >= 220.0 → exceeds
  #   overload backoff: 219.996 + 0.1   = 220.096 >= 220.0 → exceeds
  def with_non_csot_backoff_time_control
    call_count = 0
    allow(Mongo::Utils).to receive(:monotonic_time) do
      call_count += 1
      call_count == 1 ? 100.0 : 219.996
    end
    yield
  end

  def make_transient_error
    e = Mongo::Error::OperationFailure.new('transient')
    e.add_label('TransientTransactionError')
    e
  end

  def make_transient_overload_error
    e = Mongo::Error::OperationFailure.new('transient overload')
    e.add_label('TransientTransactionError')
    e.add_label('SystemOverloadedError')
    e
  end

  def make_commit_overload_error
    e = Mongo::Error::OperationFailure.new('commit overload')
    e.add_label('UnknownTransactionCommitResult')
    e.add_label('SystemOverloadedError')
    e
  end

  # --- Regular (non-overload) backoff exceeds deadline ---

  context 'when regular backoff would exceed CSOT deadline (CSOT mode)' do
    it 'raises TimeoutError with last_error as cause' do
      last = make_transient_error

      with_csot_backoff_time_control do
        expect do
          session.with_transaction(timeout_ms: 1) do
            raise last
          end
        end.to raise_error(Mongo::Error::TimeoutError) do |err|
          expect(err.cause).to eq(last)
        end
      end
    end
  end

  context 'when regular backoff would exceed the 120 s deadline (non-CSOT mode)' do
    it 'raises last_error directly (not TimeoutError)' do
      last = make_transient_error

      with_non_csot_backoff_time_control do
        expect do
          session.with_transaction do
            raise last
          end
        end.to raise_error(Mongo::Error::OperationFailure) do |err|
          expect(err).to eq(last)
          expect(err).not_to be_a(Mongo::Error::TimeoutError)
        end
      end
    end
  end

  # --- Overload backoff exceeds deadline ---

  context 'when overload backoff would exceed CSOT deadline (CSOT mode)' do
    it 'raises TimeoutError with last_error as cause' do
      last = make_transient_overload_error

      with_csot_backoff_time_control do
        expect do
          session.with_transaction(timeout_ms: 1) do
            raise last
          end
        end.to raise_error(Mongo::Error::TimeoutError) do |err|
          expect(err.cause).to eq(last)
        end
      end
    end
  end

  context 'when overload backoff would exceed the 120 s deadline (non-CSOT mode)' do
    it 'raises last_error directly (not TimeoutError)' do
      last = make_transient_overload_error

      with_non_csot_backoff_time_control do
        expect do
          session.with_transaction do
            raise last
          end
        end.to raise_error(Mongo::Error::OperationFailure) do |err|
          expect(err).to eq(last)
          expect(err).not_to be_a(Mongo::Error::TimeoutError)
        end
      end
    end
  end

  # --- Commit overload backoff exceeds deadline (CSOT only) ---

  context 'when commit overload backoff would exceed CSOT deadline (CSOT mode)' do
    it 'raises TimeoutError with the commit error as cause' do
      commit_error = make_commit_overload_error

      allow(session).to receive(:commit_transaction) do
        raise commit_error
      end

      # All monotonic_time calls return 100.0:
      #   deadline setup → 100.001
      #   pre-commit deadline check → not expired
      #   post-commit-fail deadline check → not expired (hits backoff path instead)
      #   commit overload backoff_would_exceed_deadline?(100.001, 0.1) → 100.1 >= 100.001 → true
      with_csot_backoff_time_control do
        expect do
          session.with_transaction(timeout_ms: 1) do
            session.instance_variable_set(:@state, Mongo::Session::TRANSACTION_IN_PROGRESS_STATE)
          end
        end.to raise_error(Mongo::Error::TimeoutError) do |err|
          expect(err.cause).to eq(commit_error)
        end
      end
    end
  end
end
