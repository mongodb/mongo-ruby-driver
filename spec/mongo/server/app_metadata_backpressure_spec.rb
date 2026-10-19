# frozen_string_literal: true

require 'lite_spec_helper'

describe Mongo::Server::AppMetadata do
  describe '#validated_document' do
    it 'includes backpressure: "2" at the top level' do
      metadata = described_class.new
      expect(metadata.validated_document[:backpressure]).to be '2'
    end

    it 'does not include backpressure in the client document' do
      metadata = described_class.new
      expect(metadata.validated_document[:client]).not_to have_key(:backpressure)
    end
  end
end
