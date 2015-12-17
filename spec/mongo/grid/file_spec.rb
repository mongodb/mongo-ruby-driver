require 'spec_helper'

describe Mongo::Grid::File do

  describe '#==' do

    let(:file) do
      described_class.new('test', :filename => 'test.txt')
    end

    context 'when the object is not a file' do

      it 'returns false' do
        expect(file).to_not eq('testing')
      end
    end

    context 'when the object is a file' do

      context 'when the objects are equal' do

        it 'returns true' do
          expect(file).to eq(file)
        end
      end

      context 'when the objects are not equal' do

        let(:other) do
          described_class.new('tester', :filename => 'test.txt')
        end

        it 'returns false' do
          expect(file).to_not eq(other)
        end
      end
    end
  end

  describe '#initialize' do

    let(:data_size) do
      Mongo::Grid::File::Chunk::DEFAULT_SIZE * 3
    end

    let(:data) do
      'testing'
    end

    before do
      (1..data_size).each{ |i| data << '1' }
    end

    context 'when provided data and file information' do

      let(:file) do
        described_class.new(data, :filename => 'test.txt')
      end

      it 'creates the chunks' do
        expect(file.chunks.size).to eq(4)
      end

      it 'returns data' do
        expect(file.data).to eq(data)
      end
    end

    context 'when data is a ruby file' do

      let(:ruby_file) do
        File.open(__FILE__)
      end

      let(:data) do
        ruby_file.read
      end

      let(:file) do
        described_class.new(data, :filename => File.basename(ruby_file.path))
      end

      it 'creates the chunks' do
        expect(file.chunks.size).to eq(4)
      end

      it 'returns data' do
        expect(file.data).to eq(data)
      end
    end

    context 'when data is an IO object' do

      let(:io) do
        StringIO.new('testing')
      end

      let(:file) do
        described_class.new(io, filename: "test.txt")
      end

      it 'creates the chunks' do
        expect(file.chunks).not_to be_empty
      end

      it 'returns data' do
        expect(file.data).to eq 'testing'
      end
    end

    context 'when using idiomatic ruby field names' do

      let(:time) do
        Time.now.utc
      end

      let(:file) do
        described_class.new(
          data,
          :filename => 'test.txt',
          :chunk_size => 100,
          :upload_date => time,
          :content_type => 'text/plain'
        )
      end

      it 'normalizes the chunk size name' do
        expect(file.chunk_size).to eq(100)
      end

      it 'normalizes the upload date name' do
        expect(file.upload_date).to eq(time)
      end

      it 'normalizes the content type name' do
        expect(file.content_type).to eq('text/plain')
      end
    end

    context 'when provided chunks and file information' do

      let(:file_id) do
        BSON::ObjectId.new
      end

      let(:info) do
        BSON::Document.new(
          :_id => file_id,
          :uploadDate => Time.now.utc,
          :filename => 'test.txt',
          :chunkSize => Mongo::Grid::File::Chunk::DEFAULT_SIZE,
          :length => data.length,
          :contentType => Mongo::Grid::File::Info::DEFAULT_CONTENT_TYPE
        )
      end

      let(:chunks) do
        Mongo::Grid::File::Chunk.split(
          data, Mongo::Grid::File::Info.new(info)
        ).map{ |chunk| chunk.document }
      end

      let(:file) do
        described_class.new(chunks, info)
      end

      it 'sets the chunks' do
        expect(file.chunks.size).to eq(4)
      end

      it 'assembles to data' do
        expect(file.data).to eq(data)
      end

      it 'sets the file information' do
        expect(file.info.id).to eq(info[:_id])
      end
    end
  end

  describe '#inspect' do

    let(:file) do
      described_class.new('Hi', :filename => 'test.txt')
    end

    it 'includes the filename' do
      expect(file.inspect).to include('test.txt')
    end
  end
end
