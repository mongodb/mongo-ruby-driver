require 'spec_helper'

describe Mongo::Grid::FSBucket::Stream::Write do

  let(:file) do
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

    context 'when the fs has a write concern' do

      let(:fs_options) do
        { write: { w: 3 } }
      end

      it 'uses the write concern of the fs as a default' do
        expect{
          stream.close
        }.to raise_exception(Mongo::Error::OperationFailure)
      end
    end

    context 'when provided options' do

      context 'when provided a write option' do

        let(:extra_options) do
          {
              write: { w: (WRITE_CONCERN[:w] + 1) }
          }
        end

        it 'sets the write concern' do
          expect(stream.write_concern.options).to eq(Mongo::WriteConcern.get(options[:write]).options)
        end

        context 'when chunks are inserted' do

          # it 'uses that write concern' do
          #   expect{
          #     stream.write(file)
          #   }.to raise_exception(Mongo::Error::OperationFailure)
          # end
        end

        context 'when a files document is inserted' do

          it 'uses that write concern' do
            expect{
              stream.close
            }.to raise_exception(Mongo::Error::OperationFailure)
          end
        end
      end

      context 'when provided a write concern option' do

        let(:options) do
          {
              write_concern: { w: 2 }
          }
        end

        it 'sets the write concern' do
          expect(stream.write_concern.options).to eq(Mongo::WriteConcern.get(options[:write_concern]).options)
        end

        context 'when chunks are inserted' do

          # it 'uses that write concern' do
          #   expect{
          #     stream.write(file)
          #   }.to raise_exception(Mongo::Error::OperationFailure)
          # end
        end

        context 'when a files document is inserted' do

          it 'uses that write concern' do
            expect{
              stream.close
            }.to raise_exception(Mongo::Error::OperationFailure)
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
          expect(stream.send(:metadata).metadata).to eq(options[:metadata])
        end
      end

      context 'when provided a chunk size option' do

        let(:options) do
          {
              chunk_size: 50
          }
        end

        it 'sets the chunk size' do
          expect(stream.send(:metadata).chunk_size).to eq(options[:chunk_size])
        end
      end

      context 'when provided a content type option' do

        let(:options) do
          {
              content_type: 'text/plain'
          }
        end

        it 'sets the content type' do
          expect(stream.send(:metadata).content_type).to eq(options[:content_type])
        end
      end

      context 'when provided an aliases option' do

        let(:options) do
          {
              aliases: [ 'testing-file' ]
          }
        end

        it 'sets the aliases' do
          expect(stream.send(:metadata).document[:aliases]).to eq(options[:aliases])
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

    context 'when provided an io stream' do

      before do
        stream.write(file)
        stream.close
      end

      it 'writes the contents of the stream' do
        expect(file_from_db.data.size).to eq(file.size)
      end

      it 'updates the length written' do
        expect(stream.instance_variable_get(:@length)).to eq(file.size)
      end

      it 'updates the position (n)' do
        expect(stream.instance_variable_get(:@n)).to eq(1)
      end
    end

    context 'when the stream is written to multiple times' do

      let(:file2) do
        File.open(__FILE__)
      end

      before do
        stream.write(file)
        stream.write(file2)
        stream.close
      end

      it 'writes the contents of the stream' do
        expect(file_from_db.data.size).to eq(file.size * 2)
      end

      it 'updates the length written' do
        expect(stream.instance_variable_get(:@length)).to eq(file.size * 2)
      end

      it 'updates the position (n)' do
        expect(stream.instance_variable_get(:@n)).to eq(2)
      end
    end

    context 'when the stream is closed' do

      before do
        stream.close
      end

      it 'does not allow further write' do
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
        expect(files_coll_doc['_id']).to eq(stream.send(:metadata).document['_id'])
      end

      it 'updates the length in the files collection file document' do
        expect(stream.send(:metadata).document[:length]).to eq(file.size)
      end

      it 'updates the md5 in the files collection file document' do
        expect(stream.send(:metadata).document[:md5]).to eq(md5)
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
end