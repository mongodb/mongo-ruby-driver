require 'spec_helper'

describe 'Zlib compression' do
  require_compression

  context 'when client has zlib compressor option enabled' do
    it 'compresses the message to the server' do
      # Double check that the client has zlib compression enabled
      expect(authorized_client.options[:compressors]).to include('zlib')
      expect(Mongo::Protocol::Compressed).to receive(:new).and_call_original
      expect_any_instance_of(Mongo::Protocol::Compressed).to receive(:serialize_fields).and_call_original
      expect_any_instance_of(Mongo::Protocol::Compressed).to receive(:maybe_inflate).and_call_original
      authorized_client['test'].insert_one(text: 'hello world')
    end
  end
end
