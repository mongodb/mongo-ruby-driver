require 'spec_helper'

describe Mongo::Grid::File::Chunk do

  let(:data) do
    BSON::Binary.new('testing')
  end

  let(:file_id) do
    BSON::ObjectId.new
  end

  describe '#initialize' do

    let(:chunk) do
      described_class.new(data, file_id, 5)
    end

    let(:document) do
      chunk.document
    end

    it 'sets the document data' do
      expect(document[:data]).to eq(data)
    end

    it 'sets the document files_id' do
      expect(document[:files_id]).to eq(file_id)
    end

    it 'sets the document id' do
      expect(document[:_id]).to be_a(BSON::ObjectId)
    end

    it 'sets the document n' do
      expect(document[:n]).to eq(5)
    end
  end

  describe '#to_bson' do

    let(:chunk) do
      described_class.new(data, file_id, 5)
    end

    let(:document) do
      chunk.document
    end

    it 'returns the document as bson' do
      expect(chunk.to_bson).to eq(document.to_bson)
    end
  end
end
