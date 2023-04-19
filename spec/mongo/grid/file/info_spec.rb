# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Grid::File::Info do

  describe '#==' do

    let(:upload_date) do
      Time.now.utc
    end

    let(:info) do
      described_class.new(:filename => 'test.txt', :length => 7, :uploadDate => upload_date)
    end

    context 'when the other is not a file info object' do

      it 'returns false' do
        expect(info).to_not eq('test')
      end
    end

    context 'when the other object is file info object' do

      context 'when the documents are equal' do

        it 'returns true' do
          expect(info).to eq(info)
        end
      end

      context 'when the documents are not equal' do

        let(:other) do
          described_class.new(:filename => 'testing.txt')
        end

        it 'returns false' do
          expect(info).to_not eq(other)
        end
      end
    end
  end

  describe '#initialize' do

    context 'when provided only a filename and length' do

      let(:info) do
        described_class.new(:filename => 'test.txt', :length => 7)
      end

      it 'sets the default id' do
        expect(info.id).to be_a(BSON::ObjectId)
      end

      it 'sets the upload date' do
        expect(info.upload_date).to be_a(Time)
      end

      it 'sets the chunk size' do
        expect(info.chunk_size).to eq(Mongo::Grid::File::Chunk::DEFAULT_SIZE)
      end

      it 'sets the content type' do
        expect(info.content_type).to eq(Mongo::Grid::File::Info::DEFAULT_CONTENT_TYPE)
      end
    end
  end

  describe '#inspect' do

    let(:info) do
      described_class.new(:filename => 'test.txt', :length => 7)
    end

    it 'includes the chunk size' do
      expect(info.inspect).to include(info.chunk_size.to_s)
    end

    it 'includes the filename' do
      expect(info.inspect).to include(info.filename)
    end

    it 'includes the md5' do
      expect(info.inspect).to include(info.md5.to_s)
    end

    it 'includes the id' do
      expect(info.inspect).to include(info.id.to_s)
    end
  end

  context 'when there are extra options' do

    let(:info) do
      described_class.new(:filename => 'test.txt', :extra_field => 'extra')
    end

    it 'includes them in the document written to the database' do
      expect(info.document['extra_field']).to eq('extra')
      expect(info.document[:extra_field]).to eq('extra')
    end
  end
end
