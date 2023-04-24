# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Protocol::Compressed do

  let(:original_message) { Mongo::Protocol::Query.new(SpecConfig.instance.test_db, 'protocol-test', { ping: 1 }) }
  let(:compressor) { 'zlib' }
  let(:level)      { nil }

  let(:message) do
    described_class.new(original_message, compressor, level)
  end

  let(:original_message_bytes) do
    buf = BSON::ByteBuffer.new
    original_message.send(:serialize_fields, buf)
    buf.to_s
  end

  describe '#serialize' do

    context "when using the snappy compressor" do
      require_snappy_compression
      let(:compressor) { 'snappy' }

      it "uses snappy" do
        expect(Snappy).to receive(:deflate).with(original_message_bytes).and_call_original
        message.serialize
      end
    end

    context "when using the zstd compressor" do
      require_zstd_compression
      let(:compressor) { 'zstd' }

      it "uses zstd with default compression level" do
        expect(Zstd).to receive(:compress).with(original_message_bytes).and_call_original
        message.serialize
      end
    end

    context 'when zlib compression level is not provided' do

      it 'does not set a compression level' do
        expect(Zlib::Deflate).to receive(:deflate).with(original_message_bytes, nil).and_call_original
        message.serialize
      end
    end

    context 'when zlib compression level is provided' do

      let(:level) { 1 }

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
