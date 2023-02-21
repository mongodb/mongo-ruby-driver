# frozen_string_literal: true
# encoding: utf-8

require 'spec_helper'

describe 'GridFS bucket integration' do
  let(:fs) do
    authorized_client.database.fs
  end

  describe 'UTF-8 string write' do
    let(:data) { "hello\u2210" }

    before do
      expect(data.length).not_to eq(data.bytesize)
    end

    shared_examples 'round-trips' do
      it 'round-trips' do
        stream = fs.open_upload_stream('test') do |stream|
          stream.write(data_to_write)
        end

        actual = nil
        fs.open_download_stream(stream.file_id) do |stream|
          actual = stream.read
        end

        expect(actual.encoding).to eq(Encoding::BINARY)
        expect(actual).to eq(data.b)
      end
    end

    context 'in binary encoding' do
      let(:data_to_write) do
        data.dup.force_encoding('binary').freeze
      end

      it_behaves_like 'round-trips'
    end

    context 'in UTF-8 encoding' do
      let(:data_to_write) do
        expect(data.encoding).to eq(Encoding::UTF_8)
        data.freeze
      end

      it_behaves_like 'round-trips'
    end
  end
end
