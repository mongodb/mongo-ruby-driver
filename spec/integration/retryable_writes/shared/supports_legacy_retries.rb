require_relative './performs_legacy_retries'

module SupportsLegacyRetries
  shared_examples 'it supports legacy retries' do
    context 'when client is set to use legacy retries' do
      let(:retry_writes) { false }

      context 'against a standalone server' do
        require_topology :single

        it_behaves_like 'it performs legacy retries'
      end

      context 'against a replica set or sharded cluster' do
        require_topology :replica_set, :sharded

        it_behaves_like 'it performs legacy retries'
      end
    end

    context 'when server does not support modern retries' do
      before do
        allow_any_instance_of(Mongo::Server).to receive(:retry_writes?).and_return(false)
      end

      context 'against a standalone server' do
        require_topology :single

        it_behaves_like 'it performs legacy retries'
      end

      context 'against a replica set or sharded cluster' do
        require_topology :replica_set, :sharded

        it_behaves_like 'it performs legacy retries'
      end
    end
  end
end
