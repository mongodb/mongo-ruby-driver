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

  describe '#serialize' do
    let(:bytes) { message.serialize }

    include_examples 'message with a header'

    describe 'zero' do
      let(:field) { bytes[16..19] }

      it 'does not set any bits' do
        expect(field).to be_int32(0)
      end
    end

    describe 'namespace' do
      let(:field) { bytes[20..36] }
      it 'serializes the namespace' do
        expect(field).to be_cstring(ns)
      end
    end

    describe 'number to return' do
      let(:field) { bytes[37..40] }
      it 'serializes the number to return' do
        expect(field).to be_int32(limit)
      end
    end

    describe 'cursor id' do
      let(:field) { bytes[41..48] }
      it 'serializes the cursor id' do
        expect(field).to be_int64(cursor_id)
      end
    end
  end
end

