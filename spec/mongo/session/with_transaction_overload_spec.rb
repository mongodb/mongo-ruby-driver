# frozen_string_literal: true

require 'spec_helper'

# Uses string describe to avoid RSpec/FilePath mismatch since
# this file is under spec/mongo/session/ (not spec/mongo/).
describe 'Mongo::Session#with_transaction overload retries' do
  let(:retry_policy) do
    Mongo::Retryable::RetryPolicy.new
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
    allow(session).to receive(:start_transaction) do |*_args|
      session.instance_variable_set(:@state, Mongo::Session::STARTING_TRANSACTION_STATE)
    end

    allow(session).to receive(:abort_transaction) do
      session.instance_variable_set(:@state, Mongo::Session::TRANSACTION_ABORTED_STATE)
    end

    allow(session).to receive(:commit_transaction) do
      session.instance_variable_set(:@state, Mongo::Session::TRANSACTION_COMMITTED_STATE)
    end

    allow(retry_policy).to receive(:backoff_delay).and_wrap_original do |method, attempt, **_kwargs|
      method.call(attempt, jitter: 1.0)
    end

    allow(session).to receive(:sleep)
  end

  context 'when callback raises TransientTransactionError with SystemOverloadedError' do
    it 'uses the new overload backoff' do
      call_count = 0
      expect(session).to receive(:sleep).with(0.1).once

      session.with_transaction do
        call_count += 1
        raise make_transient_overload_error if call_count == 1
      end
    end
  end

  context 'when callback raises TransientTransactionError without SystemOverloadedError' do
    it 'uses the existing backoff' do
      call_count = 0
      expect(session).to receive(:sleep).once
      expect(session).not_to receive(:sleep).with(0.1)

      session.with_transaction do
        call_count += 1
        raise make_transient_error if call_count == 1
      end
    end
  end

  context 'when overload errors exceed DEFAULT_MAX_RETRIES' do
    it 'raises the error after DEFAULT_MAX_RETRIES' do
      max_retries = Mongo::Retryable::Backpressure::DEFAULT_MAX_RETRIES
      call_count = 0

      expect do
        session.with_transaction do
          call_count += 1
          raise make_transient_overload_error
        end
      end.to raise_error(Mongo::Error::OperationFailure, 'overloaded')

      expect(call_count).to eq(max_retries + 1)
    end
  end

  context 'when overload is encountered then non-overload transient follows' do
    let(:overload_error) { make_transient_overload_error }
    let(:transient_error) { make_transient_error }

    it 'uses overload backoff for the subsequent non-overload error' do
      expect(retry_policy).to receive(:backoff_delay).twice.and_call_original

      call_count = 0
      session.with_transaction do
        call_count += 1
        case call_count
        when 1 then raise overload_error
        when 2 then raise transient_error
        end
      end
    end
  end

  context 'when commit raises UnknownTransactionCommitResult with SystemOverloadedError' do
    before do
      commit_count = 0
      allow(session).to receive(:commit_transaction) do
        commit_count += 1
        raise make_commit_overload_error if commit_count == 1

        session.instance_variable_set(:@state, Mongo::Session::TRANSACTION_COMMITTED_STATE)
      end
    end

    it 'applies overload backoff during commit retry' do
      expect(session).to receive(:sleep).once

      session.with_transaction do
        session.instance_variable_set(:@state, Mongo::Session::TRANSACTION_IN_PROGRESS_STATE)
      end
    end
  end

  context 'when commit raises TransientTransactionError with SystemOverloadedError' do
    before do
      commit_count = 0
      allow(session).to receive(:commit_transaction) do
        commit_count += 1
        raise make_commit_transient_overload_error if commit_count == 1

        session.instance_variable_set(:@state, Mongo::Session::TRANSACTION_COMMITTED_STATE)
      end
    end

    it 'tracks overload state for the next transaction attempt' do
      expect(retry_policy).to receive(:backoff_delay).once.and_call_original

      session.with_transaction do
        session.instance_variable_set(:@state, Mongo::Session::TRANSACTION_IN_PROGRESS_STATE)
      end
    end
  end

  context 'when commit overload errors exceed DEFAULT_MAX_RETRIES' do
    before do
      allow(session).to receive(:commit_transaction).and_raise(make_commit_overload_error)
    end

    it 'raises after DEFAULT_MAX_RETRIES' do
      max_retries = Mongo::Retryable::Backpressure::DEFAULT_MAX_RETRIES
      expect(retry_policy).to receive(:backoff_delay)
        .exactly(max_retries + 1).times.and_call_original

      expect do
        session.with_transaction do
          session.instance_variable_set(:@state, Mongo::Session::TRANSACTION_IN_PROGRESS_STATE)
        end
      end.to raise_error(Mongo::Error::OperationFailure, 'commit overloaded')
    end
  end
end
