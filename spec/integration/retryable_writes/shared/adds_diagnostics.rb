# frozen_string_literal: true
# rubocop:todo all

module AddsDiagnostics
  shared_examples 'it adds diagnostics' do
    it 'indicates the server used for the operation' do
      expect do
        perform_operation
      end.to raise_error(Mongo::Error, /on #{ClusterConfig.instance.primary_address_str}/)
    end

    it 'indicates the second attempt' do
      expect do
        perform_operation
      end.to raise_error(Mongo::Error, /attempt 2/)
    end
  end
end
