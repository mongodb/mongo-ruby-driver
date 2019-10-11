require 'spec_helper'

describe 'Retryable writes errors tests' do

  let(:client) do
    authorized_client.with(retry_writes: true)
  end

  let(:collection) do
    client['retryable-writes-error-spec']
  end

  context 'when the storage engine does not support retryable writes but the server does' do
    require_mmapv1
    min_server_fcv '3.6'
    require_topology :replica_set, :sharded

    before do
      collection.delete_many
    end

    context 'when a retryable write is attempted' do
      it 'raises an actionable error message' do
        expect {
          collection.insert_one(a:1)
        }.to raise_error(Mongo::Error::OperationFailure, /This MongoDB deployment does not support retryable writes. Please add retryWrites=false to your connection string or use the retry_writes: false Ruby client option/)
        expect(collection.find.count).to eq(0)
      end
    end
  end
end
