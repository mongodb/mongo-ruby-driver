# frozen_string_literal: true
# encoding: utf-8

require 'spec_helper'

class SessionTransactionSpecError < StandardError; end

describe Mongo::Session do
  require_wired_tiger
  min_server_fcv '4.0'
  require_topology :replica_set, :sharded

  let(:session) do
    authorized_client.start_session(session_options)
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

        before do
          # create collection if it does not exist
          collection.insert_one(a: 1)
        end

        it 'times out' do
          start = Mongo::Utils.monotonic_time

          10.times do |i|
            expect(Mongo::Utils).to receive(:monotonic_time).ordered.and_return(start + i)
          end
          expect(Mongo::Utils).to receive(:monotonic_time).ordered.and_return(start + 200)

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
  end
end
