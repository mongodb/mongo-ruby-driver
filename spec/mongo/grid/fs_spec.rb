require 'spec_helper'

describe Mongo::Grid::FS do

  describe '#initialize' do

    let(:fs) do
      described_class.new(authorized_client.database)
    end

    it 'sets the database' do
      expect(fs.database).to eq(authorized_client.database)
    end

    it 'sets the files collection' do
      expect(fs.files_collection.name).to eq('fs_files')
    end

    it 'sets the chunks collection' do
      expect(fs.chunks_collection.name).to eq('fs_chunks')
    end
  end

  describe '#find' do

    let(:fs) do
      described_class.new(authorized_client.database)
    end

    let(:file) do
      Mongo::Grid::File.new('Hello!', :filename => 'test.txt')
    end

    before do
      fs.insert_one(file)
    end

    after do
      fs.files_collection.find.remove_many
      fs.chunks_collection.find.remove_many
    end

    let(:from_db) do
      fs.find(:filename => 'test.txt')
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

    before do
      fs.insert_one(file)
    end

    after do
      fs.files_collection.find.remove_many
      fs.chunks_collection.find.remove_many
    end

    let(:from_db) do
      fs.find(:filename => 'test.txt')
    end

    it 'inserts the file into the database' do
      expect(from_db.filename).to eq(file.metadata.filename)
    end

    it 'includes the chunks and data with the file' do
      expect(from_db.data).to eq('Hello!')
    end
  end
end
