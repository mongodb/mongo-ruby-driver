require 'spec_helper'

describe Mongo::Grid::FS do

  describe '#initialize' do

    let(:fs) do
      described_class.new(authorized_client.database)
    end

    let(:chunks_index) do
      fs.chunks_collection.indexes.get(:files_id => 1, :n => 1)
    end

    let(:files_index) do
      fs.files_collection.indexes.get(:filename => 1, :uploadDate => 1)
    end

    it 'sets the files collection' do
      expect(fs.files_collection.name).to eq('fs.files')
    end

    it 'sets the chunks collection' do
      expect(fs.chunks_collection.name).to eq('fs.chunks')
    end

    it 'creates the index on the chunks collection' do
      expect(chunks_index[:name]).to eq('files_id_1_n_1')
    end

    it 'creates the index on the files collection' do
      expect(files_index[:name]).to eq('filename_1_uploadDate_1')
    end

    context 'when there is an OperationFailure' do

      let(:chunks_collection) do
        authorized_client.database["fs.#{Mongo::Grid::File::Chunk::COLLECTION}"]
      end

      before do
        chunks_collection.drop
        chunks_collection.indexes.create_one(Mongo::Grid::FS::CHUNKS_INDEX, unique: false)
      end

      after do
        chunks_collection.drop
      end

      it 'recovers and does not raise an exception' do
        expect(fs.chunks_collection).to eq(chunks_collection)
      end
    end

    context 'when options are provided' do

      let(:fs) do
        described_class.new(authorized_client.database, options)
      end

      context 'when a write concern is set' do

        let(:options) do
          { write: { w: 2 } }
        end

        it 'set the write concern' do
          expect(fs.send(:write_concern).options).to eq(Mongo::WriteConcern.get(w: 2).options)
        end
      end
    end
  end

  describe '#find_one' do

    let(:fs) do
      described_class.new(authorized_client.database)
    end

    let(:file) do
      Mongo::Grid::File.new('hello world!', :filename => 'test.txt')
    end

    before do
      fs.insert_one(file)
    end

    after do
      fs.files_collection.delete_many
      fs.chunks_collection.delete_many
    end

    let(:from_db) do
      fs.find_one(:filename => 'test.txt')
    end

    it 'returns the assembled file from the db' do
      expect(from_db.filename).to eq(file.metadata.filename)
    end
  end

  describe '#insert_one' do

    let(:fs) do
      described_class.new(authorized_client.database)
    end

    let(:file) do
      Mongo::Grid::File.new('Hello!', :filename => 'test.txt')
    end

    context 'when inserting the file once' do

      let!(:result) do
        fs.insert_one(file)
      end

      after do
        fs.files_collection.delete_many
        fs.chunks_collection.delete_many
      end

      let(:from_db) do
        fs.find_one(:filename => 'test.txt')
      end

      it 'inserts the file into the database' do
        expect(from_db.filename).to eq(file.metadata.filename)
      end

      it 'includes the chunks and data with the file' do
        expect(from_db.data).to eq('Hello!')
      end

      it 'returns the file id' do
        expect(result).to eq(file.id)
      end
    end

    context 'when inserting the file more than once' do

      after do
        fs.files_collection.delete_many
        fs.chunks_collection.delete_many
      end

      it 'raises an error' do
        expect {
          fs.insert_one(file)
          fs.insert_one(file)
        }.to raise_error(Mongo::Error::OperationFailure)
      end
    end

    context 'when the file exceeds the max bson size' do

      let(:fs) do
        described_class.new(authorized_client.database)
      end

      let(:file) do
        str = 'y' * 16777216
        Mongo::Grid::File.new(str, :filename => 'large-file.txt')
      end

      before do
        fs.insert_one(file)
      end

      after do
        fs.files_collection.delete_many
        fs.chunks_collection.delete_many
      end

      it 'successfully inserts the file' do
        expect(
          fs.find_one(:filename => 'large-file.txt').chunks
        ).to eq(file.chunks)
      end
    end
  end

  describe '#delete_one' do

    let(:fs) do
      described_class.new(authorized_client.database)
    end

    let(:file) do
      Mongo::Grid::File.new('Hello!', :filename => 'test.txt')
    end

    before do
      fs.insert_one(file)
      fs.delete_one(file)
    end

    let(:from_db) do
      fs.find_one(:filename => 'test.txt')
    end

    it 'removes the file from the db' do
      expect(from_db).to be_nil
    end
  end
end
