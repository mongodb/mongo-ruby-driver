# frozen_string_literal: true

# Test-only backdoor for observing handshake documents.
#
# The driver does not emit command monitoring events for commands issued during
# the handshake, so the mongodb-handshake specification permits drivers to use a
# test-only mechanism to intercept the handshake hello command for verification.
# See specifications/source/mongodb-handshake/tests/README.md.
module HandshakeDocumentRecorder
  # Records every handshake document built while the example runs.
  #
  # @return [ Array<BSON::Document> ] the recorded handshake documents
  def record_handshake_documents
    documents = []
    allow_any_instance_of(Mongo::Server::ConnectionCommon)
      .to receive(:handshake_document).and_wrap_original do |original, *args, **kwargs, &block|
        original.call(*args, **kwargs, &block).tap { |doc| documents << doc }
      end
    documents
  end
end

RSpec.configure do |config|
  config.include HandshakeDocumentRecorder
end
