require 'spec_helper'

describe 'GridFS integration tests' do
  let(:bucket) do
    authorized_client.database.fs
  end

  before do
    bucket.files_collection.delete_many
    bucket.chunks_collection.delete_many
  end

  describe 'write' do
    context 'upload_from_stream' do
      let(:content) { 'hello world' }
      let(:io) { StringIO.new(content) }

      it 'works' do
        id = bucket.upload_from_stream('hello.txt', io)
        id.should be_a(BSON::ObjectId)

        file = bucket.find_one(_id: id)
        file.should be_a(Mongo::Grid::File)

        # Not application/octet-stream
        file.info.content_type.should == 'binary/octet-stream'
        file.info.id.should == id
        file.info.md5.should be_a(String)
        file.info.md5.should =~ /\A\w{32}\z/

        file2 = bucket.find_one(filename: 'hello.txt')
        file2.should be_a(Mongo::Grid::File)
        file2.info.should == file.info
      end

      context 'with metadata' do
        let(:metadata) do
          {
            chunk_size: 10240,
            content_type: 'text/plain',
            filename: 'bar.txt',
            #md5: '1234xx',
            id: 42,
            _id: 420,
            length: 1234,
            upload_date: Time.utc(2020, 1, 1),
            aliases: %w(one two),
            metadata: {
              meta: 'data',
            }.freeze,
          }.freeze
        end

        it 'sets metadata' do
          id = bucket.upload_from_stream('hello.txt', io, metadata)
          id.should be_a(BSON::ObjectId)

          file = bucket.find_one(_id: id)
          file.should be_a(Mongo::Grid::File)

          # Respected
          file.info.chunk_size.should == 10240
          file.info.content_type.should == 'text/plain'
          file.info.filename.should == 'bar.txt'
          file.info.upload_date.should == Time.utc(2020, 1, 1)
          file.info.aliases.should == %w(one two)
          file.info.metadata.should == {'meta' => 'data'}

          # Ignored
          file.info.id.should be_a(BSON::ObjectId)
          file.info.id.should == id
          file.info.md5.should be_a(String)
          file.info.md5.should =~ /\A\w{32}\z/
          file.info.length.should == content.length

          file2 = bucket.find_one(filename: 'bar.txt')
          file2.should be_a(Mongo::Grid::File)
          file2.info.should == file.info

          file3 = bucket.find_one(filename: 'hello.txt')
          file3.should be nil
        end

        context 'when aliases is of an unusual type' do
          let(:metadata) do
            {
              aliases: 700,
            }
          end

          it 'sets preserves the type and the value' do
            id = bucket.upload_from_stream('hello.txt', io, metadata)
            id.should be_a(BSON::ObjectId)

            file = bucket.find_one(_id: id)
            file.should be_a(Mongo::Grid::File)

            file.info.aliases.should == 700
          end
        end
      end
    end
  end
end
