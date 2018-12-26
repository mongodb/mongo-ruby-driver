require 'spec_helper'

describe Mongo::Session do
  min_server_version '4.0'
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
    end
  end
end
