# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'Zlib compression' do
  require_zlib_compression

  before do
    authorized_client['test'].drop
  end

  context 'when client has zlib compressor option enabled' do
    it 'compresses the message to the server' do
      # Double check that the client has zlib compression enabled
      expect(authorized_client.options[:compressors]).to include('zlib')

      expect(Mongo::Protocol::Compressed).to receive(:new).twice.and_call_original
      expect(Zlib::Deflate).to receive(:deflate).twice.and_call_original
      expect(Zlib::Inflate).to receive(:inflate).twice.and_call_original

      authorized_client['test'].insert_one(_id: 1, text: 'hello world')
      document = authorized_client['test'].find(_id: 1).first

      expect(document['text']).to eq('hello world')
    end
  end
end
