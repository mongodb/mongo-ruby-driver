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

  describe 'message rendering' do
    let(:single_error_result) do
      { 'writeErrors' => [ { 'code' => 11_000, 'errmsg' => 'dup key' } ] }
    end

    let(:multi_error_result) do
      {
        'writeErrors' => [
          { 'code' => 11_000, 'errmsg' => 'dup' },
          { 'code' => 121, 'errmsg' => 'validation' },
        ]
      }
    end

    context 'when flag is off' do
      around do |example|
        original = Mongo.include_server_address_in_errors
        Mongo.include_server_address_in_errors = false
        example.run
      ensure
        Mongo.include_server_address_in_errors = original
      end

      it 'does not include host for single error' do
        e = described_class.new(single_error_result, server_addresses: [ 'h1:27017' ])
        expect(e.message).not_to include('on h1')
      end

      it 'does not include host for multi-error bulk' do
        e = described_class.new(multi_error_result, server_addresses: [ 'h1:27017' ])
        expect(e.message).not_to include('on h1')
      end
    end

    context 'when flag is on' do
      around do |example|
        original = Mongo.include_server_address_in_errors
        Mongo.include_server_address_in_errors = true
        example.run
      ensure
        Mongo.include_server_address_in_errors = original
      end

      it 'appends single host suffix' do
        e = described_class.new(single_error_result, server_addresses: [ 'h1:27017' ])
        expect(e.message).to end_with('(on h1:27017)')
      end

      it 'deduplicates repeated hosts' do
        e = described_class.new(multi_error_result, server_addresses: [ 'h1:27017', 'h1:27017' ])
        expect(e.message).to end_with('(on h1:27017)')
      end

      it 'joins multiple unique hosts with a comma' do
        e = described_class.new(multi_error_result, server_addresses: [ 'h1:27017', 'h2:27017' ])
        expect(e.message).to end_with('(on h1:27017, h2:27017)')
      end

      it 'omits the suffix when addresses is empty' do
        e = described_class.new(single_error_result, server_addresses: [])
        expect(e.message).not_to include('(on ')
      end
    end
  end
end
