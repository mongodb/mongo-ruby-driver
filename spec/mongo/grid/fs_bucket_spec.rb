require 'spec_helper'

describe Mongo::Grid::FSBucket do

  let(:fs) do
    described_class.new(authorized_client.database, options)
  end

  let(:options) do
    { }
  end

  let(:filename) do
    'specs.rb'
  end

  let(:file) do
    File.open(__FILE__)
  end

  describe '#initialize' do

    it 'sets the files collection' do
      expect(fs.files_collection.name).to eq('fs.files')
    end

    it 'sets the chunks collection' do
      expect(fs.chunks_collection.name).to eq('fs.chunks')
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

          it 'sets the write concern' do
            expect(fs.send(:write_concern).options).to eq(Mongo::WriteConcern.get(w: 2).options)
          end
        end
      end

      context 'when a read preference is set' do

        let(:options) do
          { read: { mode: :secondary, server_selection_timeout: 0.1 } }
        end

        let(:read_pref) do
          Mongo::ServerSelector.get(Mongo::Options::Redacted.new(options[:read].merge(authorized_client.options)))
        end

        it 'sets the read preference' do
          expect(fs.send(:read_preference)).to eq(read_pref)
        end
      end

      context 'when a write stream is opened' do

        let(:stream) do
          fs.open_upload_stream('test.txt')
        end

        let(:fs) do
          described_class.new(authorized_client.database, options)
        end

        context 'when a write option is specified' do

          let(:options) do
            { write: { w: 2 } }
          end

          it 'passes the write concern to the write stream' do
            expect(stream.write_concern.options).to eq(Mongo::WriteConcern.get(options[:write]).options)
          end
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

    context 'when provided a filter' do

      let(:view) do
        fs.find(filename: 'test.txt')
      end

      it 'returns a collection view for the filter' do
        expect(view.filter).to eq('filename' => 'test.txt')
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
      expect(from_db.filename).to eq(file.info.filename)
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
        expect(from_db.filename).to eq(file.info.filename)
      end

      it 'includes the chunks and data with the file' do
        expect(from_db.data).to eq('Hello!')
      end

      it 'returns the file id' do
        expect(result).to eq(file.id)
      end
    end

    context 'when the files collection is empty' do

      before do
        fs.files_collection.delete_many
        fs.chunks_collection.delete_many
        expect(fs.files_collection).to receive(:indexes).and_call_original
        expect(fs.chunks_collection).to receive(:indexes).and_call_original
        fs.insert_one(file)
      end

      after do
        fs.files_collection.delete_many
        fs.chunks_collection.delete_many
      end

      let(:chunks_index) do
        fs.database[fs.chunks_collection.name].indexes.get(:files_id => 1, :n => 1)
      end

      let(:files_index) do
        fs.database[fs.files_collection.name].indexes.get(:filename => 1, :uploadDate => 1)
      end

      it 'creates an index on the files collection' do
        expect(files_index[:name]).to eq('filename_1_uploadDate_1')
      end

      it 'creates an index on the chunks collection' do
        expect(chunks_index[:name]).to eq('files_id_1_n_1')
      end

      context 'when a write operation is called more than once' do

        before do
          expect(fs).not_to receive(:ensure_indexes!)
        end

        let(:file2) do
          Mongo::Grid::File.new('Goodbye!', :filename => 'test2.txt')
        end

        it 'only creates the indexes the first time' do
          expect(fs.insert_one(file2)).to be_a(BSON::ObjectId)
        end
      end
    end

    context 'when the index creation encounters an error', if: write_command_enabled? do

      before do
        fs.chunks_collection.drop
        fs.chunks_collection.indexes.create_one(Mongo::Grid::FSBucket::CHUNKS_INDEX, :unique => false)
        expect(fs.chunks_collection).to receive(:indexes).and_call_original
        expect(fs.files_collection).not_to receive(:indexes)
      end

      after do
        fs.database[fs.chunks_collection.name].indexes.drop_one('files_id_1_n_1')
      end

      it 'raises the error to the user' do
        expect {
          fs.insert_one(file)
        }.to raise_error(Mongo::Error::OperationFailure)
      end
    end

    context 'when the files collection is not empty' do

      before do
        fs.files_collection.insert_one(a: 1)
        expect(fs.files_collection).not_to receive(:indexes)
        expect(fs.chunks_collection).not_to receive(:indexes)
        fs.insert_one(file)
      end

      after do
        fs.files_collection.delete_many
        fs.chunks_collection.delete_many
      end

      let(:files_index) do
        fs.database[fs.files_collection.name].indexes.get(:filename => 1, :uploadDate => 1)
      end

      it 'assumes indexes already exist' do
        expect(files_index[:name]).to eq('filename_1_uploadDate_1')
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
        }.to raise_error(Mongo::Error::BulkWriteError)
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

  describe '#delete' do

    let(:file_id) do
      fs.upload_from_stream(filename, file)
    end

    before do
      fs.delete(file_id)
    end

    let(:from_db) do
      fs.find_one(:filename => filename)
    end

    it 'removes the file from the db' do
      expect(from_db).to be_nil
    end
  end

  context 'when a read stream is opened' do

    let(:fs) do
      described_class.new(authorized_client.database)
    end

    let(:io) do
      StringIO.new
    end

    after do
      fs.files_collection.delete_many
      fs.chunks_collection.delete_many
    end

    describe '#open_download_stream' do

      let!(:file_id) do
        fs.open_upload_stream(filename) do |stream|
          stream.write(file)
        end.file_id
      end

      context 'when a block is provided' do

        let!(:stream) do
          fs.open_download_stream(file_id) do |stream|
            io.write(stream.read)
          end
        end

        it 'returns a Stream::Read object' do
          expect(stream).to be_a(Mongo::Grid::FSBucket::Stream::Read)
        end

        it 'closes the stream after the block completes' do
          expect(stream.closed?).to be(true)
        end

        it 'yields the stream to the block' do
          expect(io.size).to eq(file.size)
        end
      end

      context 'when a block is not provided' do

        let!(:stream) do
          fs.open_download_stream(file_id)
        end

        it 'returns a Stream::Read object' do
          expect(stream).to be_a(Mongo::Grid::FSBucket::Stream::Read)
        end

        it 'does not close the stream' do
          expect(stream.closed?).to be(false)
        end

        it 'does not yield the stream to the block' do
          expect(io.size).to eq(0)
        end
      end
    end

    describe '#download_to_stream' do

      context 'when the file is found' do

        let!(:file_id) do
          fs.open_upload_stream(filename) do |stream|
            stream.write(file)
          end.file_id
        end

        before do
          fs.download_to_stream(file_id, io)
        end

        it 'writes to the provided stream' do
          expect(io.size).to eq(file.size)
        end

        it 'does not close the stream' do
          expect(io.closed?).to be(false)
        end

        context 'when the file has length 0' do

          let(:file) do
            StringIO.new('')
          end

          let(:from_db) do
            fs.open_upload_stream(filename) { |s| s.write(file) }
            fs.find_one(:filename => filename)
          end

          it 'can read the file back' do
            expect(from_db.data.size).to eq(file.size)
          end
        end
      end

      context 'when there is no files collection document found' do

        it 'raises an exception' do
          expect{
            fs.download_to_stream(BSON::ObjectId.new, io)
          }.to raise_exception(Mongo::Error::FileNotFound)
        end
      end

      context 'when a file has an id that is not an ObjectId' do

        before do
          fs.insert_one(file)
          fs.download_to_stream(file_id, io)
        end

        let(:file_id) do
          'non-object-id'
        end

        let(:file) do
          Mongo::Grid::File.new(File.open(__FILE__).read,
                                :filename => filename,
                                :_id => file_id)
        end

        it 'reads the file successfully' do
          expect(io.size).to eq(file.data.size)
        end
      end
    end

    context 'when a read preference is specified' do

      let(:fs) do
        described_class.new(authorized_client.database, options)
      end

      let(:options) do
        { read: { mode: :secondary } }
      end

      let(:stream) do
        fs.open_download_stream(BSON::ObjectId)
      end

      it 'sets the read preference on the Stream::Read object' do
        expect(stream.read_preference).to eq(Mongo::ServerSelector.get(options[:read]))
      end
    end

    describe '#download_to_stream_by_name' do

      let(:files) do
        [
            StringIO.new('hello 1'),
            StringIO.new('hello 2'),
            StringIO.new('hello 3'),
            StringIO.new('hello 4')
        ]
      end

      before do
        files.each do |file|
          fs.upload_from_stream('test.txt', file)
        end
      end

      let(:io) do
        StringIO.new
      end

      context 'when revision is not specified' do

        let!(:result) do
          fs.download_to_stream_by_name('test.txt', io)
        end

        it 'returns the most recent version' do
          expect(io.string).to eq('hello 4')
        end
      end

      context 'when revision is 0' do

        let!(:result) do
          fs.download_to_stream_by_name('test.txt', io, revision: 0)
        end

        it 'returns the original stored file' do
          expect(io.string).to eq('hello 1')
        end
      end

      context 'when revision is negative' do

        let!(:result) do
          fs.download_to_stream_by_name('test.txt', io, revision: -2)
        end

        it 'returns that number of versions from the most recent' do
          expect(io.string).to eq('hello 3')
        end
      end

      context 'when revision is positive' do

        let!(:result) do
          fs.download_to_stream_by_name('test.txt', io, revision: 1)
        end

        it 'returns that number revision' do
          expect(io.string).to eq('hello 2')
        end
      end

      context 'when the file revision is not found' do

        it 'raises a FileNotFound error' do
          expect {
            fs.download_to_stream_by_name('test.txt', io, revision: 100)
          }.to raise_exception(Mongo::Error::InvalidFileRevision)
        end
      end

      context 'when the file is not found' do

        it 'raises a FileNotFound error' do
          expect {
            fs.download_to_stream_by_name('non-existent.txt', io)
          }.to raise_exception(Mongo::Error::FileNotFound)
        end
      end
    end

    describe '#open_download_stream_by_name' do

      let(:files) do
        [
            StringIO.new('hello 1'),
            StringIO.new('hello 2'),
            StringIO.new('hello 3'),
            StringIO.new('hello 4')
        ]
      end

      before do
        files.each do |file|
          fs.upload_from_stream('test.txt', file)
        end
      end

      let(:io) do
        StringIO.new
      end

      context 'when a block is provided' do

        let(:stream) do
          fs.open_download_stream_by_name('test.txt') do |stream|
            io.write(stream.read)
          end
        end

        it 'returns a Stream::Read object' do
          expect(stream).to be_a(Mongo::Grid::FSBucket::Stream::Read)
        end

        it 'closes the stream after the block completes' do
          expect(stream.closed?).to be(true)
        end

        it 'yields the stream to the block' do
          stream
          expect(io.size).to eq(files[0].size)
        end

        context 'when revision is not specified' do

          let!(:result) do
            fs.open_download_stream_by_name('test.txt') do |stream|
              io.write(stream.read)
            end
          end

          it 'returns the most recent version' do
            expect(io.string).to eq('hello 4')
          end
        end

        context 'when revision is 0' do

          let!(:result) do
            fs.open_download_stream_by_name('test.txt', revision: 0) do |stream|
              io.write(stream.read)
            end
          end

          it 'returns the original stored file' do
            expect(io.string).to eq('hello 1')
          end
        end

        context 'when revision is negative' do

          let!(:result) do
            fs.open_download_stream_by_name('test.txt', revision: -2) do |stream|
              io.write(stream.read)
            end
          end

          it 'returns that number of versions from the most recent' do
            expect(io.string).to eq('hello 3')
          end
        end

        context 'when revision is positive' do

          let!(:result) do
            fs.open_download_stream_by_name('test.txt', revision: 1) do |stream|
              io.write(stream.read)
            end
          end

          it 'returns that number revision' do
            expect(io.string).to eq('hello 2')
          end
        end

        context 'when the file revision is not found' do

          it 'raises a FileNotFound error' do
            expect {
              fs.open_download_stream_by_name('test.txt', revision: 100)
            }.to raise_exception(Mongo::Error::InvalidFileRevision)
          end
        end

        context 'when the file is not found' do

          it 'raises a FileNotFound error' do
            expect {
              fs.open_download_stream_by_name('non-existent.txt')
            }.to raise_exception(Mongo::Error::FileNotFound)
          end
        end
      end

      context 'when a block is not provided' do

        let!(:stream) do
          fs.open_download_stream_by_name('test.txt')
        end

        it 'returns a Stream::Read object' do
          expect(stream).to be_a(Mongo::Grid::FSBucket::Stream::Read)
        end

        it 'does not close the stream' do
          expect(stream.closed?).to be(false)
        end

        it 'does not yield the stream to the block' do
          expect(io.size).to eq(0)
        end
      end
    end
  end

  context 'when a write stream is opened' do

    let(:stream) do
      fs.open_upload_stream(filename)
    end

    after do
      fs.files_collection.delete_many
      fs.chunks_collection.delete_many
    end

    describe '#open_upload_stream' do

      context 'when a block is not provided' do

        it 'returns a Stream::Write object' do
          expect(stream).to be_a(Mongo::Grid::FSBucket::Stream::Write)
        end

        it 'creates an ObjectId for the file' do
          expect(stream.file_id).to be_a(BSON::ObjectId)
        end
      end

      context 'when a block is provided' do

        let!(:stream) do
          fs.open_upload_stream(filename) do |stream|
            stream.write(file)
          end
        end

        let(:result) do
          fs.find_one(filename: filename)
        end

        it 'returns the stream' do
          expect(stream).to be_a(Mongo::Grid::FSBucket::Stream::Write)
        end

        it 'creates an ObjectId for the file' do
          expect(stream.file_id).to be_a(BSON::ObjectId)
        end

        it 'yields the stream to the block' do
          expect(result.data.size).to eq(file.size)
        end

        it 'closes the stream when the block completes' do
          expect(stream.closed?).to be(true)
        end
      end
    end

    describe '#upload_from_stream' do

      let!(:result) do
        fs.upload_from_stream(filename, file)
      end

      let(:file_from_db) do
        fs.find_one(:filename => filename)
      end

      it 'writes to the provided stream' do
        expect(file_from_db.data.length).to eq(file.size)
      end

      it 'does not close the stream' do
        expect(file.closed?).to be(false)
      end

      it 'returns the id of the file' do
        expect(result).to be_a(BSON::ObjectId)
      end

      context 'when the io stream raises an error' do

        let(:stream) do
          fs.open_upload_stream(filename)
        end

        before do
          allow(fs).to receive(:open_upload_stream).and_yield(stream)
        end

        context 'when stream#abort does not raise an OperationFailure' do

          before do
            expect(stream).to receive(:abort).and_call_original
            file.close
          end

          it 'raises the original IOError' do
            expect {
              fs.upload_from_stream(filename, file)
            }.to raise_exception(IOError)
          end
        end

        context 'when stream#abort raises an OperationFailure' do

          before do
            allow(stream).to receive(:abort).and_raise(Mongo::Error::OperationFailure)
            file.close
          end

          it 'raises the original IOError' do
            expect {
              fs.upload_from_stream(filename, file)
            }.to raise_exception(IOError)
          end
        end
      end
    end

    context 'when options are provided when opening the write stream' do

      let(:stream) do
        fs.open_upload_stream(filename, stream_options)
      end

      context 'when a write option is specified' do

        let(:stream_options) do
          { write: { w: 2 } }
        end

        it 'sets the write concern on the write stream' do
          expect(stream.write_concern.options).to eq(Mongo::WriteConcern.get(stream_options[:write]).options)
        end
      end

      context 'when there is a chunk size set on the FSBucket' do

        let(:stream_options) do
          {  }
        end

        let(:options) do
          { chunk_size: 100 }
        end

        it 'sets the chunk size as the default on the write stream' do
          expect(stream.options[:chunk_size]).to eq(options[:chunk_size])
        end
      end

      context 'when a chunk size option is specified' do

        let(:stream_options) do
          { chunk_size: 50 }
        end

        it 'sets the chunk size on the write stream' do
          expect(stream.options[:chunk_size]).to eq(stream_options[:chunk_size])
        end

        context 'when there is a chunk size set on the FSBucket' do

          let(:options) do
            { chunk_size: 100 }
          end

          let(:fs) do
            described_class.new(authorized_client.database, options)
          end

          it 'uses the chunk size set on the write stream' do
            expect(stream.options[:chunk_size]).to eq(stream_options[:chunk_size])
          end

        end
      end

      context 'when a file metadata option is specified' do

        let(:stream_options) do
          { metadata: { some_field: 1 } }
        end

        it 'sets the file metadata option on the write stream' do
          expect(stream.options[:metadata]).to eq(stream_options[:metadata])
        end
      end

      context 'when a content type option is specified' do

        let(:stream_options) do
          { content_type: 'text/plain' }
        end

        it 'sets the content type on the write stream' do
          expect(stream.options[:content_type]).to eq(stream_options[:content_type])
        end
      end

      context 'when a aliases option is specified' do

        let(:stream_options) do
          { aliases: [ 'another-name.txt' ] }
        end

        it 'sets the alias option on the write stream' do
          expect(stream.options[:aliases]).to eq(stream_options[:aliases])
        end
      end
    end
  end
end
