# frozen_string_literal: true
# rubocop:todo all

require_relative './supports_modern_retries'
require_relative './supports_legacy_retries'

module SupportsRetries
  shared_examples 'it supports retries' do
    it_behaves_like 'it supports modern retries'
    it_behaves_like 'it supports legacy retries'

    context 'when retry writes is off' do
      let(:retry_writes) { false }
      let(:max_write_retries) { 0 }

      it_behaves_like 'it performs no retries'
    end
  end
end
