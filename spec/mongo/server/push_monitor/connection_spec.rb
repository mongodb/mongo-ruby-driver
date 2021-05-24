# frozen_string_literal: true
# encoding: utf-8

require 'spec_helper'

describe Mongo::Server::PushMonitor::Connection do
  describe '#check_document' do
    it 'returns hello document with API version' do
      meta = Mongo::Server::AppMetadata.new({
        server_api: { version: '1' }
      })
      subject = described_class.new(dup(), {app_metadata: meta})
      document = subject.check_document
      expect(document['hello']).to eq(1)
    end

    it 'returns legacy hello document without API version' do
      meta = Mongo::Server::AppMetadata.new({})
      subject = described_class.new(dup(), {app_metadata: meta})
      document = subject.check_document
      expect(document['isMaster']).to eq(1)
    end
  end
end
