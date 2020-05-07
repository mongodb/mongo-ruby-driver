require_relative './performs_legacy_retries'
require_relative './performs_no_retries'

module SupportsLegacyRetries
  shared_examples 'it supports legacy retries' do
    context 'when client is set to use legacy retries' do
      let(:retry_writes) { false }

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
          it_behaves_like 'it performs legacy retries'
        end

        context 'when write concern is acknowledged' do
          let(:write_concern) { { w: :majority } }

          it_behaves_like 'it performs legacy retries'
        end

        context 'when write concern is unacknowledged' do
          let(:write_concern) { { w: 0 } }

          # it_behaves_like 'it performs no retries'
        end
      end
    end

    context 'when server does not support modern retries' do
      before do
        allow_any_instance_of(Mongo::Server).to receive(:retry_writes?).and_return(false)
      end

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
          it_behaves_like 'it performs legacy retries'
        end

        context 'when write concern is acknowledged' do
          let(:write_concern) { { w: :majority } }

          it_behaves_like 'it performs legacy retries'
        end

        context 'when write concern is unacknowledged' do
          let(:write_concern) { { w: 0 } }

          # it_behaves_like 'it performs no retries'
        end
      end
    end
  end
end
