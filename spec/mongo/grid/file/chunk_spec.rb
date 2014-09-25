require 'spec_helper'

describe Mongo::Grid::File::Chunk do

  let(:data) do
    BSON::Binary.new('testing')
  end

  let(:file_id) do
    BSON::ObjectId.new
  end

  describe '.assemble' do

  end

  describe '#document' do

    let(:chunk) do
      described_class.new(data, file_id, 5)
    end

    let(:document) do
      chunk.document
    end

    it 'sets the data' do
      expect(document[:data]).to eq(data)
    end

    it 'sets the files_id' do
      expect(document[:files_id]).to eq(file_id)
    end

    it 'sets the position' do
      expect(document[:n]).to eq(5)
    end

    it 'sets an object id' do
      expect(document[:_id]).to be_a(BSON::ObjectId)
    end

    context 'when asking for the document multiple times' do

      it 'returns the same document' do
        expect(document[:_id]).to eq(chunk.document[:_id])
      end
    end
  end

  describe '#initialize' do

    let(:chunk) do
      described_class.new(data, file_id, 5)
    end

    it 'sets the data' do
      expect(chunk.data).to eq(data)
    end

    it 'sets the file_id' do
      expect(chunk.file_id).to eq(file_id)
    end

    it 'sets the position' do
      expect(chunk.position).to eq(5)
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

  describe '.split' do

    context 'when the data is smaller than the default size' do

      let(:raw_data) do
        'testing'
      end

      let(:data) do
        BSON::Binary.new(raw_data)
      end

      let(:chunks) do
        described_class.split(raw_data, file_id)
      end

      let(:chunk) do
        chunks.first
      end

      it 'returns a single chunk' do
        expect(chunks.size).to eq(1)
      end

      it 'sets the correct chunk position' do
        expect(chunk.position).to eq(0)
      end

      it 'sets the correct chunk data' do
        expect(chunk.data).to eq(data)
      end
    end

    context 'when the data is larger that the default size' do

      let(:data_size) do
        Mongo::Grid::File::Chunk::DEFAULT_SIZE * 3
      end

      let(:raw_data) do
        'testing'
      end

      let(:data) do
        BSON::Binary.new(raw_data)
      end

      let(:assembled) do
        full_data = ''
        chunks.each do |chunk|
          full_data << chunk.data.data
        end
        full_data
      end

      before do
        (1..data_size).each{ |i| raw_data << '1' }
      end

      let(:chunks) do
        described_class.split(raw_data, file_id)
      end

      it 'returns the correct number of chunks' do
        expect(chunks.size).to eq(4)
      end

      it 'sets the correct chunk positions' do
        expect(chunks[0].position).to eq(0)
        expect(chunks[1].position).to eq(1)
        expect(chunks[2].position).to eq(2)
        expect(chunks[3].position).to eq(3)
      end

      it 'does to miss any bytes' do
        expect(assembled).to eq(raw_data)
      end
    end
  end
end
