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

  let(:messages) do
    'message1 (1), message2 (2)'
  end

  let(:notes_tail) do
    ' (note1, note2)'
  end

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
    it 'returns the formatted message' do
      expect(error.message).to eq("#{described_class}: #{messages}#{notes_tail}")
    end
  end

  describe '#to_s' do
    it 'returns the error represented as a string' do
      expect(error.to_s).to eq("#{described_class}: #{messages}#{notes_tail}")
    end
  end
end
