# frozen_string_literal: true

require 'lite_spec_helper'

describe Mongo::Error::BulkWriteError do
  let(:result) do
    {
      'writeErrors' => [
        { 'code' => 1, 'errmsg' => 'message1' },
        { 'code' => 2, 'errmsg' => 'message2' },
      ]
    }
  end

  let(:error) { described_class.new(result) }

  before do
    error.add_note('note1')
    error.add_note('note2')
  end

  describe '#result' do
    it 'returns the result' do
      expect(error.result).to eq(result)
    end
  end

  describe '#labels' do
    it 'returns an empty array' do
      expect(error.labels).to eq([])
    end
  end

  describe '#message' do
    it 'is correct' do
      expect(error.message).to eq('Multiple errors: [1]: message1; [2]: message2 (note1, note2)')
    end
  end

  describe '#to_s' do
    it 'is correct' do
      expect(error.to_s).to eq('Multiple errors: [1]: message1; [2]: message2 (note1, note2)')
    end
  end

  describe '#inspect' do
    it 'is correct' do
      expect(error.inspect).to eq('#<Mongo::Error::BulkWriteError: Multiple errors: [1]: message1; [2]: message2 (note1, note2)>')
    end
  end

  describe '#server_addresses' do
    let(:result) { { 'writeErrors' => [ { 'code' => 11_000, 'errmsg' => 'dup' } ] } }

    it 'defaults to an empty array when not supplied' do
      error = described_class.new(result)
      expect(error.server_addresses).to eq([])
    end

    it 'stores an array of strings' do
      error = described_class.new(result, server_addresses: [ 'h1:27017', 'h2:27017' ])
      expect(error.server_addresses).to eq([ 'h1:27017', 'h2:27017' ])
    end

    it 'normalizes Mongo::Address entries to seed strings' do
      addrs = [ Mongo::Address.new('h1:27017'), 'h2:27017' ]
      error = described_class.new(result, server_addresses: addrs)
      expect(error.server_addresses).to eq([ 'h1:27017', 'h2:27017' ])
    end

    it 'treats nil as empty' do
      error = described_class.new(result, server_addresses: nil)
      expect(error.server_addresses).to eq([])
    end
  end
end
