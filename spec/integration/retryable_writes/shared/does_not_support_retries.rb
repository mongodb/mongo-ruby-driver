require_relative './performs_no_retries'

module DoesNotSupportRetries
  shared_examples 'it does not support retries' do
    context 'against a standalone server' do

    end

    context 'against a replica set or sharded cluster' do
      require_topology :replica_set, :sharded

      context 'when retry_writes is true' do
        let(:retry_writes) { true }

        it_behaves_like 'it performs no retries'
      end

      context 'when retry_writes is false' do
        let(:retry_writes) { false }

        it_behaves_like 'it performs no retries'
      end

      context 'when retry_writes is false with no max_write_retries' do
        let(:retry_writes) { false }
        let(:max_write_retries) { 0 }

        it_behaves_like 'it performs no retries'
      end
    end
  end
end
