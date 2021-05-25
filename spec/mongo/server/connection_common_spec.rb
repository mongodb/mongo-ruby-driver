# frozen_string_literal: true
# encoding: utf-8

require 'spec_helper'

describe Mongo::Server::ConnectionCommon do
  let(:subject) { described_class.new }

  describe '#handshake_document' do
    it 'returns hello document with API version' do
      meta = Mongo::Server::AppMetadata.new({
        server_api: { version: '1'  }
      })
      document = subject.handshake_document(meta)
      expect(document['hello']).to eq(1)
    end

    it 'returns legacy hello document without API version' do
      meta = Mongo::Server::AppMetadata.new({})
      document = subject.handshake_document(meta)
      expect(document['isMaster']).to eq(1)
    end
  end
end
