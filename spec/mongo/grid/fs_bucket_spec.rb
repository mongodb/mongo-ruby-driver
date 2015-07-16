require 'spec_helper'

describe Mongo::Grid::FSBucket do

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
        chunks_collection.indexes.create_one(Mongo::Grid::FSBucket::CHUNKS_INDEX, unique: false)
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

        context 'when the option :write is provided' do

          let(:options) do
            { write: { w: 2 } }
          end

          it 'set the write concern' do
            expect(fs.send(:write_concern).options).to eq(Mongo::WriteConcern.get(w: 2).options)
          end
        end

        context 'when the option :write_concern is provided' do

          let(:options) do
            { write_concern: { w: 2 } }
          end

          it 'set the write concern' do
            expect(fs.send(:write_concern).options).to eq(Mongo::WriteConcern.get(w: 2).options)
          end
        end
      end

      context 'when a read preference is set' do

        let(:options) do
          { read: { mode: :secondary, server_selection_timeout: 10 } }
        end

        let(:read_pref) do
          Mongo::ServerSelector.get(options[:read].merge(authorized_client.options))
        end

        it 'sets the read preference' do
          expect(fs.send(:read_preference)).to eq(read_pref)
        end
      end
    end
  end

  describe '#find' do

    let(:fs) do
      described_class.new(authorized_client.database)
    end

    context 'when there is no selector provided' do

      let(:files) do
        [
            Mongo::Grid::File.new('hello world!', :filename => 'test.txt'),
            Mongo::Grid::File.new('goodbye world!', :filename => 'test1.txt')
        ]
      end

      before do
        files.each do |file|
          fs.insert_one(file)
        end
      end

      after do
        fs.files_collection.delete_many
        fs.chunks_collection.delete_many
      end

      it 'returns a collection view' do
        expect(fs.find).to be_a(Mongo::Collection::View)
      end

      it 'iterates over the documents in the result' do
        fs.find.each do |document|
          expect(document).to_not be_nil
        end
      end
    end

    context 'when provided a selector' do

      let(:view) do
        fs.find(filename: 'test.txt')
      end

      it 'returns a collection view for the selector' do
        expect(view.selector).to eq(filename: 'test.txt')
      end
    end

    context 'when options are provided' do

      let(:view) do
        fs.find({filename: 'test.txt'}, options)
      end

      context 'when provided batch_size' do

        let(:options) do
          { batch_size: 5 }
        end

        it 'sets the batch_size on the view' do
          expect(view.batch_size).to eq(options[:batch_size])
        end
      end

      context 'when provided limit' do

        let(:options) do
          { limit: 5 }
        end

        it 'sets the limit on the view' do
          expect(view.limit).to eq(options[:limit])
        end
      end

      context 'when provided no_cursor_timeout' do

        let(:options) do
          { no_cursor_timeout: true }
        end

        it 'sets the no_cursor_timeout on the view' do
          expect(view.options[:no_cursor_timeout]).to eq(options[:no_cursor_timeout])
        end
      end

      context 'when provided skip' do

        let(:options) do
          { skip: 5 }
        end

        it 'sets the skip on the view' do
          expect(view.skip).to eq(options[:skip])
        end
      end

      context 'when provided sort' do

        let(:options) do
          { sort:  { 'x' => Mongo::Index::ASCENDING } }
        end

        it 'sets the sort on the view' do
          expect(view.sort).to eq(options[:sort])
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
