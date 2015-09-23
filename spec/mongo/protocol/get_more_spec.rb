require 'spec_helper'

describe Mongo::Protocol::GetMore do

  let(:opcode)    { 2005 }
  let(:db)        { TEST_DB }
  let(:coll)      { TEST_COLL }
  let(:ns)        { "#{db}.#{coll}" }
  let(:limit)     { 25 }
  let(:cursor_id) { 12345 }

  let(:message) do
    described_class.new(db, coll, limit, cursor_id)
  end

  describe '#initialize' do

    it 'sets the namepsace' do
      expect(message.namespace).to eq(ns)
    end

    it 'sets the number to return' do
      expect(message.number_to_return).to eq(limit)
    end

    it 'sets the cursor id' do
      expect(message.cursor_id).to eq(cursor_id)
    end
  end

  describe '#==' do

    context 'when the other is a getmore' do

      context 'when the fields are equal' do
        let(:other) do
          described_class.new(db, coll, limit, cursor_id)
        end

        it 'returns true' do
          expect(message).to eq(other)
        end
      end

      context 'when the database is not equal' do
        let(:other) do
          described_class.new('tyler', coll, limit, cursor_id)
        end

        it 'returns false' do
          expect(message).not_to eq(other)
        end
      end

      context 'when the collection is not equal' do
        let(:other) do
          described_class.new(db, 'tyler', limit, cursor_id)
        end

        it 'returns false' do
          expect(message).not_to eq(other)
        end
      end

      context 'when the limit is not equal' do
        let(:other) do
          described_class.new(db, coll, 123, cursor_id)
        end

        it 'returns false' do
          expect(message).not_to eq(other)
        end
      end

      context 'when the cursor id is not equal' do
        let(:other) do
          described_class.new(db, coll, limit, 7777)
        end

        it 'returns false' do
          expect(message).not_to eq(other)
        end
      end
    end

    context 'when the other is not a getmore' do
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

    it 'returns true' do
      expect(message).to be_replyable
    end
  end

  describe '#serialize' do
    let(:bytes) { message.serialize }

    include_examples 'message with a header'

    describe 'zero' do
      let(:field) { bytes.to_s[16..19] }

      it 'does not set any bits' do
        expect(field).to be_int32(0)
      end
    end

    describe 'namespace' do
      let(:field) { bytes.to_s[20..36] }
      it 'serializes the namespace' do
        expect(field).to be_cstring(ns)
      end
    end

    describe 'number to return' do
      let(:field) { bytes.to_s[37..40] }
      it 'serializes the number to return' do
        expect(field).to be_int32(limit)
      end
    end

    describe 'cursor id' do
      let(:field) { bytes.to_s[41..48] }
      it 'serializes the cursor id' do
        expect(field).to be_int64(cursor_id)
      end
    end
  end
end
