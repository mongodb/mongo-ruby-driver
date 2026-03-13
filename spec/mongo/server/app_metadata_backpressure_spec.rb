# frozen_string_literal: true

require 'lite_spec_helper'

describe Mongo::Server::AppMetadata do
  describe '#client_document' do
    it 'includes backpressure: true' do
      metadata = described_class.new
      expect(metadata.client_document[:backpressure]).to be true
    end
  end
end
