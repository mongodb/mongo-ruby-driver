require 'spec_helper'

describe Mongo::Grid::File::Metadata do

  describe '#initialize' do

    context 'when provided only a filename and length' do

      let(:metadata) do
        described_class.new(:filename => 'test.txt', :length => 7)
      end

      it 'sets the default id' do
        expect(metadata.id).to be_a(BSON::ObjectId)
      end

      it 'sets the upload date' do
        expect(metadata.upload_date).to be_a(Time)
      end

      it 'sets the chunk size' do
        expect(metadata.chunk_size).to eq(Mongo::Grid::File::Chunk::DEFAULT_SIZE)
      end

      it 'sets the content type' do
        expect(metadata.content_type).to eq(Mongo::Grid::File::Metadata::DEFAULT_CONTENT_TYPE)
      end
    end
  end
end
