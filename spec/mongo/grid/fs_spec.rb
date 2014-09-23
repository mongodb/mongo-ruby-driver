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
      expect(fs.files.name).to eq('fs_files')
    end

    it 'sets the chunks collection' do
      expect(fs.chunks.name).to eq('fs_chunks')
    end
  end
end
