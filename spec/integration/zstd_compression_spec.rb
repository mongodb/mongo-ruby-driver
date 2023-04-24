# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'Zstd compression' do
  min_server_version '4.2'
  require_zstd_compression

  before do
    authorized_client['test'].drop
  end

  context 'when client has snappy compressor option enabled' do
    it 'compresses the message to the server' do
      # Double check that the client has zstd compression enabled
      expect(authorized_client.options[:compressors]).to include('zstd')

      expect(Mongo::Protocol::Compressed).to receive(:new).twice.and_call_original
      expect(Zstd).to receive(:compress).twice.and_call_original
      expect(Zstd).to receive(:decompress).twice.and_call_original

      authorized_client['test'].insert_one(_id: 1, text: 'hello world')
      document = authorized_client['test'].find(_id: 1).first

      expect(document['text']).to eq('hello world')
    end
  end
end
