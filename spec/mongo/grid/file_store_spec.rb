require 'spec_helper'

describe Mongo::Grid::FS do

  let(:files)    { authorized_client[:fs_files] }
  let(:chunks)   { authorized_client[:fs_chunks] }
  let(:filename) { "test-grid-file.txt" }
  let(:msg)      { "The rain in Spain falls mainly on the plains" }
  let(:id)       { BSON::ObjectId.new }

  let(:grid)     { described_class.new(files, chunks) }
  let(:f_w) { grid.open(filename, 'w') }

  pending '#open' do

    it 'returns a Grid::File' do
      expect(grid.open(filename, 'w')).to be_a(Mongo::Grid::File)
    end

    context 'mode is neither r nor w' do

      it 'raises an error' do
        expect{ grid.open(filename, 'aaaa') }.to raise_error
      end
    end
  end

  pending '#delete' do

    before do
      f_w.write(msg)
    end

    it 'returns an Integer' do
      expect(grid.delete(filename)).to be(1)
    end

    context 'id is a filename' do

      it 'deletes all of the matching files' do
        expect(grid.delete(filename)).to be(1)
      end
    end

    context 'id is an ObjectId' do

      it 'deletes one file' do
        expect(grid.delete(f_w.files_id)).to be(1)
      end
    end
  end

  pending '#exists?' do

    context 'when id is a filename' do

      context 'when file exists' do

        it 'returns true' do
          f_w.write(msg)
          expect(grid.exists?(filename)).to be(true)
        end
      end

      context 'when the file does not exist' do

        it 'returns false' do
          expect(grid.exists?(filename)).to be(false)
        end
      end
    end

    context 'when id is an ObjectId' do

      context 'when file exists' do

        it 'returns true' do
          f = grid.open(filename, 'w')
          expect(grid.exists?(f.files_id)).to be(true)
        end
      end

      context 'when file does not exist' do

        it 'returns false' do
          expect(grid.exists?(id)).to be(false)
        end
      end
    end
  end

  pending '#size' do

    it 'returns an Integer' do
      expect(grid.size).to be_a(Integer)
    end

    it 'returns the number of files in the system' do
      f = grid.open(filename, 'w')
      expect(grid.size).to be(1)
    end
  end

  pending '#find' do

    it 'returns an array of Grid::File objects' do
      f = grid.open(filename, 'w')
      files = grid.find({ :filename => filename })
      expect(files[0]).to be_a(Mongo::Grid::File)
    end

    it 'returns only all files that match query' do
      f = grid.open(filename, 'w')
      f_2 = grid.open("Another-file", 'w')
      expect(grid.find({ :filename => filename }).length).to be(1)
    end
  end
end
