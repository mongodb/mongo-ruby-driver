# frozen_string_literal: true

require 'lite_spec_helper'

describe Mongo::StreamProcessing::SamplesResult do
  describe '#exhausted?' do
    it 'returns true when cursor_id is zero' do
      expect(described_class.new(0, []).exhausted?).to be true
    end

    it 'returns false when cursor_id is non-zero' do
      expect(described_class.new(42, []).exhausted?).to be false
    end
  end

  describe '#documents' do
    it 'returns the documents array' do
      docs = [ { 'x' => 1 }, { 'x' => 2 } ]
      expect(described_class.new(7, docs).documents).to eq(docs)
    end

    it 'defaults to an empty array when given nil' do
      expect(described_class.new(7, nil).documents).to eq([])
    end
  end
end
