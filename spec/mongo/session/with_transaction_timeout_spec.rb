# frozen_string_literal: true

require 'spec_helper'

# Prose tests for the "Retry Timeout is Enforced" and "Backoff Deadline is
# Enforced" sections of the transactions-convenient-api spec README.
#
# specifications/source/transactions-convenient-api/tests/README.md
#
# Note 1 from spec: "The error SHOULD be propagated as a timeout error if
# the language allows to expose the underlying error as a cause of a timeout
# error." Ruby supports this via Exception#cause.
describe 'Mongo::Session#with_transaction timeout enforcement' do
  let(:retry_policy) { Mongo::Retryable::RetryPolicy.new }

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

  # Stubs Mongo::Utils.monotonic_time: first `initial_calls` invocations
  # return 100.0 (deadline ≈ 100.001 s with timeout_ms: 1), all subsequent
  # calls return 200.0, making every deadline check return "expired".
  def with_expired_deadline_after(initial_calls:)
    call_count = 0
    allow(Mongo::Utils).to receive(:monotonic_time) do
      call_count += 1
      (call_count <= initial_calls) ? 100.0 : 200.0
    end
    yield
  end

  # CSOT time control: monotonic_time always 100.0.
  # With timeout_ms: 1, deadline = 100.001.
  # Backoffs (0.005 s, 0.1 s) exceed that deadline; deadline_expired? stays false.
  def with_csot_backoff_time_control
    allow(Mongo::Utils).to receive(:monotonic_time).and_return(100.0)
    allow(Random).to receive(:rand).and_return(1.0)
    yield
  end

  # non-CSOT time control: first call → 100.0 (deadline = 220.0),
  # subsequent calls → 219.996.
  # deadline_expired? = false; backoffs (0.005, 0.1) exceed the 220.0 deadline.
  def with_non_csot_backoff_time_control
    call_count = 0
    allow(Mongo::Utils).to receive(:monotonic_time) do
      call_count += 1
      (call_count == 1) ? 100.0 : 219.996
    end
    allow(Random).to receive(:rand).and_return(1.0)
    yield
  end

  def make_transient_error
    Mongo::Error::OperationFailure.new('transient').tap do |e|
      e.add_label('TransientTransactionError')
    end
  end

  def make_commit_unknown_error
    Mongo::Error::OperationFailure.new('commit unknown').tap do |e|
      e.add_label('UnknownTransactionCommitResult')
    end
  end

  def make_commit_transient_error
    Mongo::Error::OperationFailure.new('commit transient').tap do |e|
      e.add_label('TransientTransactionError')
    end
  end

  def make_transient_overload_error
    Mongo::Error::OperationFailure.new('transient overload').tap do |e|
      e.add_label('TransientTransactionError')
      e.add_label('SystemOverloadedError')
    end
  end

  def make_commit_overload_error
    Mongo::Error::OperationFailure.new('commit overload').tap do |e|
      e.add_label('UnknownTransactionCommitResult')
      e.add_label('SystemOverloadedError')
    end
  end

  # ---------------------------------------------------------------------------
  # "Retry Timeout is Enforced" — three sub-cases from the spec README
  # ---------------------------------------------------------------------------

  describe '"Retry Timeout is Enforced" prose tests' do
    context 'when callback raises TransientTransactionError and retry timeout is exceeded' do
      let(:transient_error) { make_transient_error }

      it 'propagates the error as TimeoutError with the same labels as the wrapped error' do
        with_expired_deadline_after(initial_calls: 1) do
          ex = expect { session.with_transaction(timeout_ms: 1) { raise transient_error } }
          ex.to raise_error(Mongo::Error::TimeoutError) do |e|
            expect(e.message).to include(transient_error.message)
            expect(e.labels).to match_array(transient_error.labels)
          end
        end
      end
    end

    context 'when commit raises UnknownTransactionCommitResult and retry timeout is exceeded' do
      let(:commit_error) { make_commit_unknown_error }

      before { allow(session).to receive(:commit_transaction) { raise commit_error } }

      it 'propagates the error as TimeoutError with the same labels as the wrapped error' do
        with_expired_deadline_after(initial_calls: 2) do
          ex = expect do
            session.with_transaction(timeout_ms: 1) do
              session.instance_variable_set(:@state, Mongo::Session::TRANSACTION_IN_PROGRESS_STATE)
            end
          end
          ex.to raise_error(Mongo::Error::TimeoutError) do |e|
            expect(e.message).to include(commit_error.message)
            expect(e.labels).to match_array(commit_error.labels)
          end
        end
      end
    end

    context 'when commit raises TransientTransactionError and retry timeout is exceeded' do
      let(:commit_error) { make_commit_transient_error }

      before { allow(session).to receive(:commit_transaction) { raise commit_error } }

      it 'propagates the error as TimeoutError with the same labels as the wrapped error' do
        with_expired_deadline_after(initial_calls: 2) do
          ex = expect do
            session.with_transaction(timeout_ms: 1) do
              session.instance_variable_set(:@state, Mongo::Session::TRANSACTION_IN_PROGRESS_STATE)
            end
          end
          ex.to raise_error(Mongo::Error::TimeoutError) do |e|
            expect(e.message).to include(commit_error.message)
            expect(e.labels).to match_array(commit_error.labels)
          end
        end
      end
    end
  end

  # ---------------------------------------------------------------------------
  # "Backoff Deadline is Enforced" — backoff-would-exceed-deadline paths
  # ---------------------------------------------------------------------------

  describe '"Backoff Deadline is Enforced" prose tests' do
    before do
      allow(retry_policy).to receive(:backoff_delay).and_wrap_original do |m, attempt, **_|
        m.call(attempt, jitter: 1.0)
      end
    end

    context 'when regular backoff would exceed CSOT deadline' do
      let(:last_error) { make_transient_error }

      it 'raises TimeoutError including last_error message' do
        with_csot_backoff_time_control do
          ex = expect { session.with_transaction(timeout_ms: 1) { raise last_error } }
          ex.to raise_error(Mongo::Error::TimeoutError) { |e| expect(e.message).to include(last_error.message) }
        end
      end
    end

    context 'when regular backoff would exceed the 120 s deadline (non-CSOT)' do
      let(:last_error) { make_transient_error }

      it 'raises last_error directly (not TimeoutError)' do
        with_non_csot_backoff_time_control do
          ex = expect { session.with_transaction { raise last_error } }
          ex.to raise_error(Mongo::Error::OperationFailure) do |e|
            expect(e).to eq(last_error)
            expect(e).not_to be_a(Mongo::Error::TimeoutError)
          end
        end
      end
    end

    context 'when overload backoff would exceed CSOT deadline' do
      let(:last_error) { make_transient_overload_error }

      it 'raises TimeoutError including last_error message' do
        with_csot_backoff_time_control do
          ex = expect { session.with_transaction(timeout_ms: 1) { raise last_error } }
          ex.to raise_error(Mongo::Error::TimeoutError) { |e| expect(e.message).to include(last_error.message) }
        end
      end
    end

    context 'when overload backoff would exceed the 120 s deadline (non-CSOT)' do
      let(:last_error) { make_transient_overload_error }

      it 'raises last_error directly (not TimeoutError)' do
        with_non_csot_backoff_time_control do
          ex = expect { session.with_transaction { raise last_error } }
          ex.to raise_error(Mongo::Error::OperationFailure) do |e|
            expect(e).to eq(last_error)
            expect(e).not_to be_a(Mongo::Error::TimeoutError)
          end
        end
      end
    end

    context 'when commit overload backoff would exceed CSOT deadline' do
      let(:commit_error) { make_commit_overload_error }

      before { allow(session).to receive(:commit_transaction) { raise commit_error } }

      it 'raises TimeoutError including the commit error message' do
        with_csot_backoff_time_control do
          ex = expect do
            session.with_transaction(timeout_ms: 1) do
              session.instance_variable_set(:@state, Mongo::Session::TRANSACTION_IN_PROGRESS_STATE)
            end
          end
          ex.to raise_error(Mongo::Error::TimeoutError) { |e| expect(e.message).to include(commit_error.message) }
        end
      end
    end
  end
end
