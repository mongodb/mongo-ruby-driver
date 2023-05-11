# frozen_string_literal: true
# rubocop:todo all

require_relative './performs_modern_retries'
require_relative './performs_no_retries'

module SupportsModernRetries
  shared_examples 'it supports modern retries' do
    let(:retry_writes) { true }

    context 'against a standalone server' do
      require_topology :single

      before(:all) do
        skip 'RUBY-2171: standalone topology currently uses legacy write retries ' \
          'by default. Standalone should NOT retry when modern retries are enabled.'
      end

      it_behaves_like 'it performs no retries'
    end

    context 'against a replica set or sharded cluster' do
      require_topology :replica_set, :sharded

      it_behaves_like 'it performs modern retries'
    end
  end
end
