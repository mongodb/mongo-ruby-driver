require 'spec_helper'

describe 'GridFS integration tests' do
  let(:bucket) do
    authorized_client.database.fs
  end

  describe 'write' do
    context 'upload_from_stream' do
      it 'works' do
        io = StringIO.new('hello world')
        id = bucket.upload_from_stream('hello.txt', io)
        id.should be_a(BSON::ObjectId)

        file = bucket.find_one(_id: id)
        file.should be_a(Mongo::Grid::File)

        file = bucket.find_one(filename: 'hello.txt')
        file.should be_a(Mongo::Grid::File)
      end
    end
  end
end
