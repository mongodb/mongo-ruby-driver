require 'lite_spec_helper'

describe Mongo::Protocol::Compressed do

  let(:original_message) { Mongo::Protocol::Query.new(SpecConfig.instance.test_db, 'protocol-test', { ping: 1 }) }
  let(:compressor) { 'zlib' }
  let(:level)      { nil }

  let(:message) do
    described_class.new(original_message, compressor, level)
  end

  describe '#serialize' do

    context 'when zlib compression level is not provided' do

      let(:original_message_bytes) do
        buf = BSON::ByteBuffer.new
        original_message.send(:serialize_fields, buf)
        buf.to_s
      end

      it 'does not set a compression level' do
        expect(Zlib::Deflate).to receive(:deflate).with(original_message_bytes, nil).and_call_original
        message.serialize
      end
    end

    context 'when zlib compression level is provided' do

      let(:level) { 1 }

      let(:original_message_bytes) do
        buf = BSON::ByteBuffer.new
        original_message.send(:serialize_fields, buf)
        buf.to_s
      end

      it 'uses the compression level' do
        expect(Zlib::Deflate).to receive(:deflate).with(original_message_bytes, 1).and_call_original
        message.serialize
      end
    end
  end

  describe '#replyable?' do

    context 'when the original message is replyable' do

      it 'returns true' do
        expect(message.replyable?).to be(true)
      end
    end

    context 'when the original message is not replyable' do

      let(:original_message) do
        Mongo::Protocol::Msg.new([:more_to_come], {}, { ping: 1 })
      end

      it 'returns false' do
        expect(message.replyable?).to be(false)
      end
    end
  end
end
