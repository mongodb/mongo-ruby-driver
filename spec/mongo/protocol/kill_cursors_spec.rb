require 'spec_helper'

describe Mongo::Protocol::KillCursors do

  let(:opcode)     { 2007 }
  let(:cursor_ids) { [123, 456, 789] }
  let(:id_count)   { cursor_ids.size }
  let(:message) do
    described_class.new(cursor_ids)
  end

  describe '#initialize' do

    it 'sets the cursor ids' do
      expect(message.cursor_ids).to eq(cursor_ids)
    end

    it 'sets the count' do
      expect(message.id_count).to eq(id_count)
    end
  end

  describe '#serialize' do
    let(:bytes) { message.serialize }

    include_examples 'message with a header'

    describe 'zero' do
      let(:field) { bytes[16..19] }

      it 'serializes a zero' do
        expect(field).to be_int32(0)
      end
    end

    describe 'number of cursors' do
      let(:field) { bytes[20..23] }
      it 'serializes the cursor count' do
        expect(field).to be_int32(id_count)
      end
    end

    describe 'cursor ids' do
      let(:field) { bytes[24..-1] }
      it 'serializes the selector' do
        expect(field).to be_int64_sequence(cursor_ids)
      end
    end
  end
end
