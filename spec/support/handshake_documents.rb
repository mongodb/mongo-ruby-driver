# frozen_string_literal: true

# Test-only backdoor for observing handshake documents.
#
# The driver does not emit command monitoring events for commands issued during
# the handshake, so the mongodb-handshake specification permits drivers to use a
# test-only mechanism to intercept the handshake hello command for verification.
# See specifications/source/mongodb-handshake/tests/README.md.
module HandshakeDocumentRecording
  # Thread-safe collector for handshake documents.
  #
  # Monitor connections perform their handshakes on background threads, so
  # appends and reads must be synchronized. On JRuby a concurrent append can
  # otherwise surface as a nil element when iterating.
  class Recorder
    def initialize
      @documents = []
      @mutex = Mutex.new
    end

    def record(document)
      @mutex.synchronize { @documents << document }
    end

    # @return [ Array<BSON::Document> ] a snapshot of the documents recorded so far
    def documents
      @mutex.synchronize { @documents.dup }
    end
  end

  # Records every handshake document built while the example runs.
  #
  # @return [ Recorder ] the recorder
  def record_handshake_documents
    recorder = Recorder.new
    allow_any_instance_of(Mongo::Server::ConnectionCommon)
      .to receive(:handshake_document).and_wrap_original do |original, *args, **kwargs, &block|
        original.call(*args, **kwargs, &block).tap { |document| recorder.record(document) }
      end
    recorder
  end
end

RSpec.configure do |config|
  config.include HandshakeDocumentRecording
end
