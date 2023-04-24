# frozen_string_literal: true
# rubocop:todo all

require_relative './performs_legacy_retries'

module SupportsLegacyRetries
  shared_examples 'it supports legacy retries' do
    context 'when server does not support modern retries' do
      before do
        allow_any_instance_of(Mongo::Server).to receive(:retry_writes?).and_return(false)
      end

      it_behaves_like 'it performs legacy retries'
    end

    context 'when client is set to use legacy retries' do
      let(:retry_writes) { false }

      it_behaves_like 'it performs legacy retries'
    end
  end
end
