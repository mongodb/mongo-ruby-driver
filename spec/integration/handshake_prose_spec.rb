# frozen_string_literal: true

require 'spec_helper'

# Prose tests from the MongoDB handshake specification:
# specifications/source/mongodb-handshake/tests/README.md
describe 'Handshake prose tests' do
  # The test-only recorder reads handshake documents while monitor connections
  # write them on background threads. That races on JRuby, so only run it on MRI.
  require_mri
  clean_slate

  # Test 9: Handshake documents include `backpressure: "2"`.
  describe 'handshake backpressure' do
    it 'includes backpressure: "2" in every handshake document' do
      documents = record_handshake_documents

      authorized_client.database.command(ping: 1)

      expect(documents).not_to be_empty
      documents.each do |document|
        expect(document['backpressure']).to eq('2')
      end
    end
  end
end
