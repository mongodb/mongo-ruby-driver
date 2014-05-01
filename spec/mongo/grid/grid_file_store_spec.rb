require 'spec_helper'

describe Mongo::Grid::GridFileStore do
  include_context 'gridfs implementation'

  it_behaves_like 'a storable object'

  describe '#delete' do

    context 'when given a filename' do

      before(:each) do
        grid.delete(filename)
      end

      it 'removes matching docs from the files collection' do
        expect(files.find_one({ '_id' => meta['_id'] })).to be_nil
      end

      it 'removes chunks of each matching file' do
        expect(chunks.find_one({ 'files_id' => meta['_id'] })).to be_nil
      end
    end

    context 'when given an ObjectId' do

      before(:each) do
        grid.delete(meta['_id'])
      end

      it 'removes matching metadata document from files collection' do
        expect(files.find_one({ '_id' => meta['_id'] })).to be_nil
      end

      it 'removes all chunks from matching file' do
        expect(chunks.find_one({ 'files_id' => meta['_id'] })).to be_nil
      end
    end
  end
end
