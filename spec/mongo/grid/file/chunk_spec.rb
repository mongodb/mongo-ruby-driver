require 'spec_helper'
require 'stringio'

describe Mongo::Grid::File::Chunk do

  let(:data) do
    BSON::Binary.new('testing')
  end

  let(:file_id) do
    BSON::ObjectId.new
  end

  let(:file_info) do
    Mongo::Grid::File::Info.new(:files_id => file_id)
  end

  describe '#==' do

    let(:chunk) do
      described_class.new(:data => data, :files_id => file_id, :n => 5)
    end

    context 'when the other is not a chunk' do

      it 'returns false' do
        expect(chunk).to_not eq('test')
      end
    end

    context 'when the other object is a chunk' do

      context 'when the documents are equal' do

        it 'returns true' do
          expect(chunk).to eq(chunk)
        end
      end

      context 'when the documents are not equal' do

        let(:other) do
          described_class.new(:data => data, :files_id => file_id, :n => 6)
        end

        it 'returns false' do
          expect(chunk).to_not eq(other)
        end
      end
    end
  end

  describe '.assemble' do

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
      described_class.assemble(chunks)
    end

    before do
      (1..data_size).each{ |i| raw_data << '1' }
    end

    let(:chunks) do
      described_class.split(raw_data, file_info)
    end

    it 'returns the chunks assembled into the raw data' do
      expect(assembled).to eq(raw_data)
    end
  end

  describe '#document' do

    let(:chunk) do
      described_class.new(:data => data, :files_id => file_id, :n => 5)
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
      described_class.new(:data => data, :files_id => file_id, :n => 5)
    end

    it 'sets the document' do
      expect(chunk.data).to eq(data)
    end

    it 'sets a default id' do
      expect(chunk.id).to be_a(BSON::ObjectId)
    end
  end

  describe '#to_bson' do

    let(:chunk) do
      described_class.new(:data => data, :files_id => file_id, :n => 5)
    end

    let(:document) do
      chunk.document
    end

    it 'returns the document as bson' do
      expect(chunk.to_bson.to_s).to eq(document.to_bson.to_s)
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
        described_class.split(raw_data, file_info)
      end

      let(:chunk) do
        chunks.first
      end

      it 'returns a single chunk' do
        expect(chunks.size).to eq(1)
      end

      it 'sets the correct chunk position' do
        expect(chunk.n).to eq(0)
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
        described_class.split(raw_data, file_info)
      end

      it 'returns the correct number of chunks' do
        expect(chunks.size).to eq(4)
      end

      it 'sets the correct chunk positions' do
        expect(chunks[0].n).to eq(0)
        expect(chunks[1].n).to eq(1)
        expect(chunks[2].n).to eq(2)
        expect(chunks[3].n).to eq(3)
      end

      it 'does to miss any bytes' do
        expect(assembled).to eq(raw_data)
      end
    end
  end
end
