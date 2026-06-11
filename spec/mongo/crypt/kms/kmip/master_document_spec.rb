# frozen_string_literal: true

require 'mongo'
require 'lite_spec_helper'

describe Mongo::Crypt::KMS::KMIP::MasterKeyDocument do
  let(:document) do
    described_class.new(options).to_document
  end

  context 'with key_id and endpoint' do
    let(:options) do
      { key_id: '1', endpoint: 'localhost:5698' }
    end

    it 'builds the libmongocrypt document' do
      expect(document).to eq(
        BSON::Document.new(provider: 'kmip', endpoint: 'localhost:5698', keyId: '1')
      )
    end

    it 'does not include delegated' do
      expect(document).not_to have_key(:delegated)
    end
  end

  context 'with delegated set to true' do
    let(:options) do
      { delegated: true }
    end

    it 'includes delegated in the document' do
      expect(document[:delegated]).to be true
    end

    it 'does not require key_id or endpoint' do
      expect(document).to eq(
        BSON::Document.new(provider: 'kmip', delegated: true)
      )
    end
  end

  context 'with delegated set to false' do
    let(:options) do
      { key_id: '1', delegated: false }
    end

    it 'includes delegated false in the document' do
      expect(document[:delegated]).to be false
    end
  end

  context 'without delegated' do
    let(:options) do
      { key_id: '1' }
    end

    it 'omits delegated from the document' do
      expect(document).not_to have_key(:delegated)
    end
  end

  context 'with a non-boolean delegated' do
    let(:options) do
      { delegated: 'yes' }
    end

    it 'raises an error' do
      expect do
        described_class.new(options)
      end.to raise_error(ArgumentError, /delegated/)
    end
  end
end
