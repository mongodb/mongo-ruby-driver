# frozen_string_literal: true
# rubocop:todo all

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
      expect(error.message).to eq("Multiple errors: [1]: message1; [2]: message2 (note1, note2)")
    end
  end

  describe '#to_s' do
    it 'is correct' do
      expect(error.to_s).to eq("Multiple errors: [1]: message1; [2]: message2 (note1, note2)")
    end
  end

  describe '#inspect' do
    it 'is correct' do
      expect(error.inspect).to eq("#<Mongo::Error::BulkWriteError: Multiple errors: [1]: message1; [2]: message2 (note1, note2)>")
    end
  end
end
