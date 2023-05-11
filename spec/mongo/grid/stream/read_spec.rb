# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Grid::FSBucket::Stream::Read do

  let(:support_fs) do
    authorized_client.database.fs(fs_options)
  end

  before do
    support_fs.files_collection.drop rescue nil
    support_fs.chunks_collection.drop rescue nil
  end

  let(:fs_options) do
    { }
  end

  let(:fs) do
    authorized_client.database.fs(fs_options)
  end

  let(:options) do
    { file_id: file_id }
  end

  let(:filename) do
    'specs.rb'
  end

  let!(:file_id) do
    fs.upload_from_stream(filename, File.open(__FILE__))
  end

  let(:stream) do
    described_class.new(fs, options)
  end

  describe '#initialize' do

    it 'sets the file id' do
      expect(stream.file_id).to eq(file_id)
    end

    it 'sets the fs object' do
      expect(stream.fs).to eq(fs)
    end

    context 'when there is a read preference set on the FSBucket' do

      let(:fs_options) do
        { read: { mode: :secondary } }
      end

      it 'uses the read preference of the fs as a default' do
        expect(stream.read_preference).to eq(fs.read_preference)
      end
    end

    it 'opens a stream' do
      expect(stream.close).to eq(file_id)
    end

    context 'when provided options' do

      context 'when provided read preference' do

        context 'when given as a hash with symbol keys' do
          let(:options) do
            {
                file_id: file_id,
                read: { mode: :primary_preferred },
            }
          end

          it 'sets the read preference as a BSON::Document' do
            expect(stream.read_preference).to be_a(BSON::Document)
            expect(stream.read_preference).to eq('mode' => :primary_preferred)
          end

          it 'sets the read preference on the view' do
            expect(stream.send(:view).read).to eq(BSON::Document.new(options[:read]))
          end
        end

        context 'when given as a BSON::Document' do
          let(:options) do
            BSON::Document.new(
              file_id: file_id,
              read: { mode: :primary_preferred },
            )
          end

          it 'sets the read preference' do
            expect(stream.read_preference).to eq(options[:read])
          end

          it 'sets the read preference on the view' do
            expect(stream.send(:view).read).to eq(options[:read])
          end
        end
      end

      context 'when provided a file_id' do

        it 'sets the file id' do
          expect(stream.file_id).to eq(options[:file_id])
        end
      end
    end
  end

  describe '#each' do

    let(:filename) do
      'specs.rb'
    end

    let!(:file_id) do
      fs.upload_from_stream(filename, File.open(__FILE__))
    end

    let(:fs_options) do
      { chunk_size: 5 }
    end

    it 'iterates over all the chunks of the file' do
      stream.each do |chunk|
        expect(chunk).not_to be(nil)
      end
    end

    context 'when the stream is closed' do

      before do
        stream.close
      end

      it 'does not allow further iteration' do
        expect {
          stream.to_a
        }.to raise_error(Mongo::Error::ClosedStream)
      end
    end

    context 'when a chunk is found out of order' do

      before do
        view = stream.fs.chunks_collection.find({ :files_id => file_id }, options).sort(:n => -1)
        stream.instance_variable_set(:@view, view)
        expect(stream).to receive(:close)
      end

      it 'raises an exception' do
        expect {
          stream.to_a
        }.to raise_error(Mongo::Error::MissingFileChunk)
      end

      it 'closes the query' do
        begin
          stream.to_a
        rescue Mongo::Error::MissingFileChunk
        end
      end
    end

    context 'when a chunk does not have the expected length' do

      before do
        stream.send(:file_info)
        stream.instance_variable_get(:@file_info).document['chunkSize'] = 4
        expect(stream).to receive(:close)
      end

      it 'raises an exception' do
        expect {
          stream.to_a
        }.to raise_error(Mongo::Error::UnexpectedChunkLength)
      end

      it 'closes the query' do
        begin
          stream.to_a
        rescue Mongo::Error::UnexpectedChunkLength
        end
      end
    end

    context 'when there is no files document found' do

      before do
        fs.files_collection.delete_many
      end

      it 'raises an Exception' do
        expect{
          stream.to_a
        }.to raise_exception(Mongo::Error::FileNotFound)
      end
    end
  end

  describe '#read' do

    let(:filename) do
      'specs.rb'
    end

    let(:file) do
      File.open(__FILE__)
    end

    let(:file_id) do
      fs.upload_from_stream(filename, file)
    end

    it 'returns a string of all data' do
      expect(stream.read.size).to eq(file.size)
    end
  end

  describe '#file_info' do

    it 'returns a files information document' do
      expect(stream.file_info).to be_a(Mongo::Grid::File::Info)
    end
  end

  describe '#close' do

    let(:view) do
      stream.instance_variable_get(:@view)
    end

    before do
      stream.to_a
    end

    it 'returns the file id' do
      expect(stream.close).to eq(file_id)
    end

    context 'when the stream is closed' do

      before do
        stream.to_a
        expect(view).to receive(:close_query).and_call_original
      end

      it 'calls close_query on the view' do
        expect(stream.close).to be_a(BSON::ObjectId)
      end
    end

    context 'when the stream is already closed' do

      before do
        stream.close
      end

      it 'does not raise an exception' do
        expect {
          stream.close
        }.not_to raise_error
      end
    end
  end

  describe '#closed?' do

    context 'when the stream is closed' do

      before do
        stream.close
      end

      it 'returns true' do
        expect(stream.closed?).to be(true)
      end
    end

    context 'when the stream is still open' do

      it 'returns false' do
        expect(stream.closed?).to be(false)
      end
    end
  end
end
