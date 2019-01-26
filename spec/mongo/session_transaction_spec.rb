require 'spec_helper'

class SessionTransactionSpecError < StandardError; end

describe Mongo::Session do
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
  end
end
