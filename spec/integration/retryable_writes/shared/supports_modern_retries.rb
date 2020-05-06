require_relative './performs_modern_retries'
require_relative './performs_no_retries'

module SupportsModernRetries
  shared_examples 'it supports modern retries' do
    let(:retry_writes) { true }

    context 'against a standalone server' do
      require_topology :single

      before do
        skip "TODO: put ticket info here"
      end

      it_behaves_like 'it performs no retries'
    end

    context 'against a replica set or sharded cluster' do
      require_topology :replica_set, :sharded

      context 'when write concern is default' do
        it_behaves_like 'it performs modern retries'
      end

      context 'when write concern is acknowledged' do
        let(:write_concern) { { w: :majority } }

        it_behaves_like 'it performs modern retries'
      end

      context 'when write concern is unacknowledged' do
        let(:write_concern) { { w: 0 } }

        # it_behaves_like 'it performs no retries'
      end
    end
  end
end
