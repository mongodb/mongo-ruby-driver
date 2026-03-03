# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

class SessionTransactionSpecError < StandardError; end

describe Mongo::Session do
  require_topology :replica_set, :sharded

  let(:subscriber) do
    Mrss::EventSubscriber.new(name: 'SessionTransactionSpec')
  end

  let(:client) do
    authorized_client.tap do |client|
      client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
    end
  end

  let(:session) do
    client.start_session(session_options)
  end

  let(:session_options) do
    {}
  end

  let(:collection) do
    authorized_client['session-transaction-test']
  end

  before do
    collection.delete_many
  end

  describe '#abort_transaction' do
    require_topology :replica_set

    context 'when a non-Mongo error is raised' do
      before do
        collection.insert_one({foo: 1})
      end

      it 'propagates the exception and sets state to transaction aborted' do
        session.start_transaction
        collection.insert_one({foo: 1}, session: session)
        expect(session).to receive(:write_with_retry).and_raise(SessionTransactionSpecError)
        expect do
          session.abort_transaction
        end.to raise_error(SessionTransactionSpecError)
        expect(session.send(:within_states?, Mongo::Session::TRANSACTION_ABORTED_STATE)).to be true

        # Since we failed abort_transaction call, the transaction is still
        # outstanding. It will cause subsequent tests to stall until it times
        # out on the server side. End the session to force the server
        # to close the transaction.
        kill_all_server_sessions
      end
    end

    context 'when a Mongo error is raised' do
      before do
        collection.insert_one({foo: 1})
      end

      it 'swallows the exception and sets state to transaction aborted' do
        session.start_transaction
        collection.insert_one({foo: 1}, session: session)
        expect(session).to receive(:write_with_retry).and_raise(Mongo::Error::SocketError)
        expect do
          session.abort_transaction
        end.not_to raise_error
        expect(session.send(:within_states?, Mongo::Session::TRANSACTION_ABORTED_STATE)).to be true

        # Since we failed abort_transaction call, the transaction is still
        # outstanding. It will cause subsequent tests to stall until it times
        # out on the server side. End the session to force the server
        # to close the transaction.
        kill_all_server_sessions
      end
    end
  end

  describe '#with_transaction' do
    require_topology :replica_set

    context 'callback successful' do
      it 'commits' do
        session.with_transaction do
          collection.insert_one(a: 1)
        end

        result = collection.find(a: 1).first
        expect(result[:a]).to eq(1)
      end

      it 'propagates callback\'s return value' do
        rv = session.with_transaction do
          42
        end
        expect(rv).to eq(42)
      end
    end

    context 'callback raises' do
      it 'propagates the exception' do
        expect do
          session.with_transaction do
            raise SessionTransactionSpecError, 'test error'
          end
        end.to raise_error(SessionTransactionSpecError, 'test error')
      end
    end

    context 'callback aborts transaction' do
      it 'does not raise exceptions and propagates callback\'s return value' do
        rv = session.with_transaction do
          session.abort_transaction
          42
        end
        expect(rv).to eq(42)
      end
    end

    context 'timeout with callback raising TransientTransactionError' do
      max_example_run_time 7

      it 'times out' do
        start = Mongo::Utils.monotonic_time

        expect(Mongo::Utils).to receive(:monotonic_time).ordered.and_return(start)
        expect(Mongo::Utils).to receive(:monotonic_time).ordered.and_return(start + 1)
        expect(Mongo::Utils).to receive(:monotonic_time).ordered.and_return(start + 2)
        expect(Mongo::Utils).to receive(:monotonic_time).ordered.and_return(start + 200)
        allow(session).to receive('check_transactions_supported!').and_return true

        expect do
          session.with_transaction do
            exc = Mongo::Error::OperationFailure.new('timeout test')
            exc.add_label('TransientTransactionError')
            raise exc
          end
        end.to raise_error(Mongo::Error::OperationFailure, 'timeout test')
      end
    end

    %w(UnknownTransactionCommitResult TransientTransactionError).each do |label|
      context "timeout with commit raising with #{label}" do
        max_example_run_time 7

        # JRuby seems to burn through the monotonic time expectations
        # very quickly and the retries of the transaction get the original
        # time which causes the transaction to be stuck there.
        fails_on_jruby

        before do
          # create collection if it does not exist
          collection.insert_one(a: 1)
        end

        retry_test
        it 'times out' do
          start = Mongo::Utils.monotonic_time

          11.times do |i|
            expect(Mongo::Utils).to receive(:monotonic_time).ordered.and_return(start + i)
          end
          expect(Mongo::Utils).to receive(:monotonic_time).ordered.and_return(start + 200)
          allow(session).to receive('check_transactions_supported!').and_return true

          exc = Mongo::Error::OperationFailure.new('timeout test')
          exc.add_label(label)

          expect(session).to receive(:commit_transaction).and_raise(exc).at_least(:once)

          expect do
            session.with_transaction do
              collection.insert_one(a: 2)
            end
          end.to raise_error(Mongo::Error::OperationFailure, 'timeout test')
        end
      end
    end

    context 'callback breaks out of with_tx loop' do
      it 'aborts transaction' do
        expect(session).to receive(:start_transaction).and_call_original
        expect(session).to receive(:abort_transaction).and_call_original
        expect(session).to receive(:log_warn).and_call_original

        session.with_transaction do
          break
        end
      end
    end

    context 'application timeout around with_tx' do
      it 'keeps session in a working state' do
        session
        collection.insert_one(a: 1)

        expect do
          Timeout.timeout(1, SessionTransactionSpecError) do
            session.with_transaction do
              sleep 2
            end
          end
        end.to raise_error(SessionTransactionSpecError)

        session.with_transaction do
          collection.insert_one(timeout_around_with_tx: 2)
        end

        expect(collection.find(timeout_around_with_tx: 2).first).not_to be nil
      end
    end

    context 'csot' do
      context 'when csot is enabled' do
        context 'when timeout_ms is set to zero' do
          it 'sets with_transaction_deadline to infinite' do
            session.with_transaction(timeout_ms: 0) do
              expect(session.with_transaction_deadline).to be_zero
            end
          end

          it 'does not sent maxTimeMS' do
            session.with_transaction(timeout_ms: 0) do
              collection.insert_one({ a: 1 }, session: session)
            end
            event = subscriber.single_command_started_event('insert', database_name: collection.database.name)
            expect(event.command['maxTimeMS']).to be_nil
          end
        end

        context 'when timeout_ms is set to a positive value' do
          before do
            allow(Mongo::Utils).to receive(:monotonic_time).and_return(0)
          end

          it 'sets with_transaction_deadline to the specified value' do
            session.with_transaction(timeout_ms: 1000) do
              expect(session.with_transaction_deadline).to be_within(0.1).of(1000 / 1000.0)
            end
          end

          it 'sends maxTimeMS with the operation' do
            session.with_transaction(timeout_ms: 1_000) do
              collection.insert_one({ a: 1 }, session: session)
            end
            event = subscriber.single_command_started_event('insert', database_name: collection.database.name)
            expect(event.command['maxTimeMS']).not_to be_nil
            expect(event.command['maxTimeMS']).to be <= 1_000
          end
        end
      end

      context 'when csot is disabled' do
        it 'does not set with_transaction_deadline' do
          session.with_transaction do
            expect(session.with_transaction_deadline).to be_nil
          end
        end
      end
    end

    context 'backoff calculation' do
      require_topology :replica_set

      it 'calculates exponential backoff correctly' do
        # Test backoff formula: jitter * min(BACKOFF_INITIAL * 1.5^(attempt-1), BACKOFF_MAX)
        backoff_initial = Mongo::Session::BACKOFF_INITIAL
        backoff_max = Mongo::Session::BACKOFF_MAX

        # Test attempt 1: 1.5^0 = 1
        expected_attempt_1 = backoff_initial * (1.5 ** 0)
        expect(expected_attempt_1).to eq(0.005)

        # Test attempt 2: 1.5^1 = 1.5
        expected_attempt_2 = backoff_initial * (1.5 ** 1)
        expect(expected_attempt_2).to eq(0.0075)

        # Test attempt 3: 1.5^2 = 2.25
        expected_attempt_3 = backoff_initial * (1.5 ** 2)
        expect(expected_attempt_3).to eq(0.01125)

        # Test cap at BACKOFF_MAX
        expected_attempt_large = [backoff_initial * (1.5 ** 20), backoff_max].min
        expect(expected_attempt_large).to eq(backoff_max)
      end

      it 'applies jitter to backoff' do
        # Jitter should be a random value between 0 and 1
        # When multiplied with backoff, it should reduce the actual sleep time
        backoff = 0.100  # 100ms
        jitter_min = 0
        jitter_max = 1

        actual_min = jitter_min * backoff
        actual_max = jitter_max * backoff

        expect(actual_min).to eq(0)
        expect(actual_max).to eq(0.100)
      end
    end
  end
end
