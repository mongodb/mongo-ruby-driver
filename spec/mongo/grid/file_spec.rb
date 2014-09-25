require 'spec_helper'

describe Mongo::Grid::File do

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

    context 'when provided data and metadata' do

      let(:file) do
        described_class.new(data, :filename => 'test.txt')
      end

      it 'sets the data' do
        expect(file.data).to eq(data)
      end

      it 'creates the chunks' do
        expect(file.chunks.size).to eq(4)
      end
    end

    context 'when provided chunks and metadata' do

      let(:file_id) do
        BSON::ObjectId.new
      end

      let(:metadata) do
        BSON::Document.new(
          :_id => file_id,
          :uploadDate => Time.now.utc,
          :filename => 'test.txt',
          :chunkSize => Mongo::Grid::File::Chunk::DEFAULT_SIZE,
          :length => data.length,
          :contentType => Mongo::Grid::File::Metadata::DEFAULT_CONTENT_TYPE
        )
      end

      let(:chunks) do
        Mongo::Grid::File::Chunk.split(data, file_id).map{ |chunk| chunk.document }
      end

      let(:file) do
        described_class.new(chunks, metadata)
      end

      it 'sets the chunks' do
        expect(file.chunks.size).to eq(4)
      end

      it 'assembles to data' do
        expect(file.data).to eq(data)
      end

      it 'sets the metadata' do
        expect(file.metadata.document).to eq(metadata)
      end
    end
  end
end
