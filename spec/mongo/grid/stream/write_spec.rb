require 'spec_helper'

describe Mongo::Grid::FSBucket::Stream::Write do

  let(:file) do
    File.open(__FILE__)
  end

  let(:file2) do
    File.open(__FILE__)
  end

  let(:fs_options) do
    { }
  end

  let(:fs) do
    authorized_client.database.fs(fs_options)
  end

  let(:filename) do
    'specs.rb'
  end

  let(:extra_options) do
    { }
  end

  let(:options) do
    { filename: filename }.merge(extra_options)
  end

  after do
    fs.files_collection.delete_many
    fs.chunks_collection.delete_many
  end

  let(:stream) do
    described_class.new(fs, options)
  end

  describe '#initialize' do

    it 'sets the file id' do
      expect(stream.file_id).to be_a(BSON::ObjectId)
    end

    it 'sets the fs object' do
      expect(stream.fs).to eq(fs)
    end

    it 'opens a stream' do
      expect(stream.close).to be_a(BSON::ObjectId)
    end

    context 'when the fs has a write concern', if: standalone? do

      let(:fs_options) do
        { write: INVALID_WRITE_CONCERN }
      end

      it 'uses the write concern of the fs as a default' do
        expect{
          stream.close
        }.to raise_exception(Mongo::Error::OperationFailure)
      end
    end

    context 'when the fs does not have a write concern' do

      let(:fs) do
        authorized_client.with(write: nil).database.fs
      end

      it 'uses the write concern default at the operation level' do
        expect(stream.write(file).closed?).to eq(false)
      end
    end

    context 'when provided options' do

      context 'when provided a write option' do

        let(:extra_options) do
          {
            write: INVALID_WRITE_CONCERN
          }
        end

        let(:expected) do
          Mongo::WriteConcern.get(options[:write]).options
        end

        it 'sets the write concern' do
          expect(stream.write_concern.options).to eq(expected)
        end

        context 'when chunks are inserted' do

          it 'uses that write concern' do
            expect(stream.send(:chunks_collection).write_concern.options[:w]).to eq(expected[:w])
          end
        end

        context 'when a files document is inserted' do

          it 'uses that write concern' do
            expect(stream.send(:files_collection).write_concern.options[:w]).to eq(expected[:w])
          end
        end
      end

      context 'when provided a metadata document' do

        let(:options) do
          {
              metadata: { 'some_field' => 'test-file' }
          }
        end

        it 'sets the metadata document' do
          expect(stream.send(:file_info).metadata).to eq(options[:metadata])
        end
      end

      context 'when provided a chunk size option' do

        let(:options) do
          {
              chunk_size: 50
          }
        end

        it 'sets the chunk size' do
          expect(stream.send(:file_info).chunk_size).to eq(options[:chunk_size])
        end

        context 'when chunk size is also set on the FSBucket object' do

          let(:fs_options) do
            {
                chunk_size: 100
            }
          end

          it 'uses the write stream options' do
            expect(stream.send(:file_info).chunk_size).to eq(options[:chunk_size])
          end
        end
      end

      context 'when provided a content type option' do

        let(:options) do
          {
              content_type: 'text/plain'
          }
        end

        it 'sets the content type' do
          expect(stream.send(:file_info).content_type).to eq(options[:content_type])
        end
      end

      context 'when provided an aliases option' do

        let(:options) do
          {
              aliases: [ 'testing-file' ]
          }
        end

        it 'sets the aliases' do
          expect(stream.send(:file_info).document[:aliases]).to eq(options[:aliases])
        end
      end

      context 'when provided a file_id option' do

        let(:options) do
          {
            file_id: 'Custom ID'
          }
        end

        it 'assigns the stream the file id' do
          expect(stream.file_id).to eq(options[:file_id])
        end
      end
    end
  end

  describe '#write' do

    after do
      fs.files_collection.delete_many
      fs.chunks_collection.delete_many
    end

    let(:file_from_db) do
      fs.find_one(filename: filename)
    end

    context 'when the stream is written to' do

      before do
        stream.write(file)
      end

      it 'does not close the stream' do
        expect(stream).not_to receive(:close)
      end
    end

    context 'when indexes need to be ensured' do

      context 'when the files collection is empty' do

        before do
          fs.files_collection.delete_many
          fs.chunks_collection.delete_many
          expect(fs.files_collection).to receive(:indexes).and_call_original
          expect(fs.chunks_collection).to receive(:indexes).and_call_original
          stream.write(file)
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

        context 'when write is called more than once' do

          before do
            expect(fs).not_to receive(:ensure_indexes!)
          end

          it 'only creates the indexes the first time' do
            stream.write(file2)
          end
        end
      end

      context 'when the files collection is not empty' do

        before do
          fs.files_collection.insert_one(a: 1)
          expect(fs.files_collection).not_to receive(:indexes)
          expect(fs.chunks_collection).not_to receive(:indexes)
          stream.write(file)
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
            stream.write(file)
          }.to raise_error(Mongo::Error::OperationFailure)
        end
      end
    end

    context 'when provided an io stream' do

      context 'when no file id is specified' do

        before do
          stream.write(file)
          stream.close
        end

        it 'writes the contents of the stream' do
          expect(file_from_db.data.size).to eq(file.size)
        end

        it 'updates the length written' do
          expect(stream.send(:file_info).document['length']).to eq(file.size)
        end

        it 'updates the position (n)' do
          expect(stream.instance_variable_get(:@n)).to eq(1)
        end
      end

      context 'when a custom file id is provided' do

        let(:extra_options) do
          {
            file_id: 'Custom ID'
          }
        end

        let!(:id) do
          stream.write(file)
          stream.close
        end

        it 'writes the contents of the stream' do
          expect(file_from_db.data.size).to eq(file.size)
        end

        it 'updates the length written' do
          expect(stream.send(:file_info).document['length']).to eq(file.size)
        end

        it 'updates the position (n)' do
          expect(stream.instance_variable_get(:@n)).to eq(1)
        end

        it 'uses the custom file id' do
          expect(id).to eq(options[:file_id])
        end
      end

      context 'when the user file contains no data' do

        before do
          stream.write(file)
          stream.close
        end

        let(:file) do
          StringIO.new('')
        end

        let(:files_coll_doc) do
          stream.fs.files_collection.find(filename: filename).to_a.first
        end

        let(:chunks_documents) do
          stream.fs.chunks_collection.find(files_id: stream.file_id).to_a
        end

        it 'creates a files document' do
          expect(files_coll_doc).not_to be(nil)
        end

        it 'sets length to 0 in the files document' do
          expect(files_coll_doc['length']).to eq(0)
        end

        it 'does not insert any chunks' do
          expect(file_from_db.data.size).to eq(file.size)
        end
      end
    end

    context 'when the stream is written to multiple times' do

      before do
        stream.write(file)
        stream.write(file2)
        stream.close
      end

      it 'writes the contents of the stream' do
        expect(file_from_db.data.size).to eq(file.size * 2)
      end

      it 'updates the length written' do
        expect(stream.send(:file_info).document['length']).to eq(file.size * 2)
      end

      it 'updates the position (n)' do
        expect(stream.instance_variable_get(:@n)).to eq(2)
      end
    end

    context 'when the stream is closed' do

      before do
        stream.close
      end

      it 'does not allow further writes' do
        expect {
          stream.write(file)
        }.to raise_error(Mongo::Error::ClosedStream)
      end
    end
  end

  describe '#close' do

    let(:file_content) do
      File.open(__FILE__).read
    end

    context 'when close is called on the stream' do

      before do
        stream.write(file)
      end

      let(:file_id) do
        stream.file_id
      end

      it 'returns the file id' do
        expect(stream.close).to eq(file_id)
      end
    end

    context 'when the stream is closed' do

      before do
        stream.write(file)
        stream.close
      end

      let(:md5) do
        Digest::MD5.new.update(file_content).hexdigest
      end

      let(:files_coll_doc) do
        stream.fs.files_collection.find(filename: filename).to_a.first
      end

      it 'inserts a file documents in the files collection' do
        expect(files_coll_doc['_id']).to eq(stream.file_id)
      end

      it 'updates the length in the files collection file document' do
        expect(stream.send(:file_info).document[:length]).to eq(file.size)
      end

      it 'updates the md5 in the files collection file document' do
        expect(stream.send(:file_info).document[:md5]).to eq(md5)
      end
    end

    context 'when the stream is already closed' do

      before do
        stream.close
      end

      it 'raises an exception' do
        expect {
          stream.close
        }.to raise_error(Mongo::Error::ClosedStream)
      end
    end
  end

  describe '#closed?' do

    context 'when the stream is closed' do

      before do
        stream.close
      end

      it 'returns true' do
        expect(stream.closed?).to be(true)
      end
    end

    context 'when the stream is still open' do

      it 'returns false' do
        expect(stream.closed?).to be(false)
      end
    end
  end
end
