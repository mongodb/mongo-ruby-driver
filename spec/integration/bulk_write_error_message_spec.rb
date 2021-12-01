require 'spec_helper'

describe 'BulkWriteError message' do
  let(:client) { authorized_client }
  let(:collection_name) { 'bulk_write_error_message_spec' }
  let(:collection) { client[collection_name] }

  before do
    collection.delete_many
  end

  context 'a bulk write with one error' do
    it 'reports code name, code and message' do
      begin
        collection.insert_many([
          {_id: 1},
          {_id: 1},
          {_id: 1},
        ], ordered: true)
        fail('Should have raised')
      rescue Mongo::Error::BulkWriteError => e
        e.message.should =~ %r,\A\[11000\]: (insertDocument :: caused by :: 11000 )?E11000 duplicate key error (collection|index):,
      end
    end
  end

  context 'a bulk write with multiple errors' do
    it 'reports code name, code and message' do
      begin
        collection.insert_many([
          {_id: 1},
          {_id: 1},
          {_id: 1},
        ], ordered: false)
        fail('Should have raised')
      rescue Mongo::Error::BulkWriteError => e
        e.message.should =~ %r,\AMultiple errors: \[11000\]: (insertDocument :: caused by :: 11000 )?E11000 duplicate key error (collection|index):.*\[11000\]: (insertDocument :: caused by :: 11000 )?E11000 duplicate key error (collection|index):,
      end
    end
  end
end
