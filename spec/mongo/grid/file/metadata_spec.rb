require 'spec_helper'

describe Mongo::Grid::File::Metadata do

  describe '#==' do

    let(:upload_date) do
      Time.now.utc
    end

    let(:metadata) do
      described_class.new(:filename => 'test.txt', :length => 7, :uploadDate => upload_date)
    end

    context 'when the other is not metadata' do

      it 'returns false' do
        expect(metadata).to_not eq('test')
      end
    end

    context 'when the other object is metadata' do

      context 'when the documents are equal' do

        it 'returns true' do
          expect(metadata).to eq(metadata)
        end
      end

      context 'when the documents are not equal' do

        let(:other) do
          described_class.new(:filename => 'testing.txt')
        end

        it 'returns false' do
          expect(metadata).to_not eq(other)
        end
      end
    end
  end

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

  describe '#inspect' do

    let(:metadata) do
      described_class.new(:filename => 'test.txt', :length => 7)
    end

    it 'includes the chunk size' do
      expect(metadata.inspect).to include(metadata.chunk_size.to_s)
    end

    it 'includes the filename' do
      expect(metadata.inspect).to include(metadata.filename)
    end

    it 'includes the md5' do
      expect(metadata.inspect).to include(metadata.md5.to_s)
    end

    it 'includes the id' do
      expect(metadata.inspect).to include(metadata.id.to_s)
    end
  end
end
