require 'spec_helper'

describe Mongo::Grid::FSBucket do

  let(:fs) do
    described_class.new(authorized_client.database, options)
  end

  let(:options) do
    { }
  end

  describe '#initialize' do

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

    context 'when there is an OperationFailure', if: write_command_enabled? do

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

      it 'raises the exception' do
        expect {
          fs
        }.to raise_exception(Mongo::Error::OperationFailure)
      end
    end

    context 'when the user is not authorized to create an index' do

      let(:authorized_fs) do
        described_class.new(authorized_client.database, options)
      end

      let(:read_user) do
        Mongo::Auth::User.new(
            user: 'read-only',
            password: 'reading',
            roles: [ Mongo::Auth::Roles::READ ]
        )
      end

      let(:filename) do
        'some-file'
      end

      before do
        authorized_fs.upload_from_stream(filename, StringIO.new('hello!'))
        root_authorized_client.database.users.create(read_user)
      end

      after do
        authorized_fs.files_collection.delete_many
        authorized_fs.chunks_collection.delete_many
        root_authorized_client.database.users.remove(read_user.name)
      end

      let(:read_db) do
        authorized_client.with(user: read_user.name, password: read_user.password).database
      end

      let(:fs) do
        described_class.new(read_db, options)
      end

      it 'recovers and does not raise an exception' do
        expect{
          fs
        }.not_to raise_exception
      end

      it 'allows the user to read from the GridFS anyway' do
        expect(fs.find_one(filename: filename)).to be_a(Mongo::Grid::File)
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
          Mongo::ServerSelector.get(options[:read].merge(authorized_client.options))
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

        context 'when a write concern option is specified' do

          let(:options) do
            { write_concern: { w: 2 } }
          end

          it 'passes the write concern to the write stream' do
            expect(stream.write_concern.options).to eq(Mongo::WriteConcern.get(options[:write_concern]).options)
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

  context 'when a read stream is opened' do

    let(:fs) do
      described_class.new(authorized_client.database)
    end

    let(:io) do
      StringIO.new
    end

    let(:file) do
      File.open(__FILE__)
    end

    let(:filename) do
      'specs.rb'
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
      end

      context 'when there is no files collection document found' do

        it 'raises an exception' do
          expect{
            fs.download_to_stream(BSON::ObjectId.new, io)
          }.to raise_exception(Mongo::Error::NoFileInfo)
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
  end

  context 'when a write stream is opened' do

    let(:filename) do
      'specs.rb'
    end

    let(:file) do
      File.open(__FILE__)
    end

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

    describe 'upload_from_stream' do

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

       context 'when a write concern option is specified' do
         let(:stream_options) do
           { write_concern: { w: 2 } }
         end

         it 'sets the write concern on the write stream' do
           expect(stream.write_concern.options).to eq(Mongo::WriteConcern.get(stream_options[:write_concern]).options)
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
