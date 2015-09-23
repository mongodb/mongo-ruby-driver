require 'spec_helper'

describe Mongo::Protocol::KillCursors do

  let(:opcode)     { 2007 }
  let(:cursor_ids) { [123, 456, 789] }
  let(:id_count)   { cursor_ids.size }
  let(:collection) { TEST_COLL }
  let(:database)   { TEST_DB }
  let(:message) do
    described_class.new(collection, database, cursor_ids)
  end

  describe '#initialize' do

    it 'sets the cursor ids' do
      expect(message.cursor_ids).to eq(cursor_ids)
    end

    it 'sets the count' do
      expect(message.id_count).to eq(id_count)
    end
  end

  describe '#==' do

    context 'when the other is a killcursors' do

      context 'when the cursor ids are equal' do
        let(:other) do
          described_class.new(collection, database, cursor_ids)
        end

        it 'returns true' do
          expect(message).to eq(other)
        end
      end

      context 'when the cursor ids are not equal' do
        let(:other) do
          described_class.new(collection, database, [123, 456])
        end

        it 'returns false' do
          expect(message).not_to eq(other)
        end
      end
    end

    context 'when the other is not a killcursors' do
      let(:other) do
        expect(message).not_to eq('test')
      end
    end
  end

  describe '#hash' do
    let(:values) do
      message.send(:fields).map do |field|
        message.instance_variable_get(field[:name])
      end
    end

    it 'returns a hash of the field values' do
      expect(message.hash).to eq(values.hash)
    end
  end

  describe '#replyable?' do

    it 'returns false' do
      expect(message).to_not be_replyable
    end
  end

  describe '#serialize' do
    let(:bytes) { message.serialize }

    include_examples 'message with a header'

    describe 'zero' do
      let(:field) { bytes.to_s[16..19] }

      it 'serializes a zero' do
        expect(field).to be_int32(0)
      end
    end

    describe 'number of cursors' do
      let(:field) { bytes.to_s[20..23] }
      it 'serializes the cursor count' do
        expect(field).to be_int32(id_count)
      end
    end

    describe 'cursor ids' do
      let(:field) { bytes.to_s[24..-1] }
      it 'serializes the selector' do
        expect(field).to be_int64_sequence(cursor_ids)
      end
    end
  end
end
