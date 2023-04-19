# frozen_string_literal: true
# rubocop:todo all

require_relative './performs_no_retries'
require_relative './performs_legacy_retries'

module OnlySupportsLegacyRetries
  shared_examples 'it only supports legacy retries' do
    context 'when retry_writes is true' do
      let(:retry_writes) { true }

      it_behaves_like 'it performs no retries'
    end

    context 'when retry_writes is false' do
      let(:retry_writes) { false }

      it_behaves_like 'it performs legacy retries'
    end

    context 'when retry_writes is false with no max_write_retries' do
      let(:retry_writes) { false }
      let(:max_write_retries) { 0 }

      it_behaves_like 'it performs no retries'
    end
  end
end
