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
      collection.insert_many([
                               { _id: 1 },
                               { _id: 1 },
                               { _id: 1 },
                             ], ordered: true)
      raise('Should have raised')
    rescue Mongo::Error::BulkWriteError => e
      e.message.should =~ /\A\[11000\]: (insertDocument :: caused by :: 11000 )?E11000 duplicate key error (collection|index):/
    end
  end

  context 'a bulk write with multiple errors' do
    it 'reports code name, code and message' do
      collection.insert_many([
                               { _id: 1 },
                               { _id: 1 },
                               { _id: 1 },
                             ], ordered: false)
      raise('Should have raised')
    rescue Mongo::Error::BulkWriteError => e
      e.message.should =~ /\AMultiple errors: \[11000\]: (insertDocument :: caused by :: 11000 )?E11000 duplicate key error (collection|index):.*\[11000\]: (insertDocument :: caused by :: 11000 )?E11000 duplicate key error (collection|index):/
    end
  end

  context 'a bulk write with validation errors' do
    let(:collection_name) { 'bulk_write_error_validation_message_spec' }

    let(:collection) do
      client[:collection_name].drop
      client[:collection_name,
             {
               'validator' => {
                 'x' => { '$type' => 'string' },
               }
             }].create
      client[:collection_name]
    end

    it 'reports code name, code, message, and details' do
      collection.insert_one({ _id: 1, x: '1' })
      collection.insert_many([
                               { _id: 1, x: '1' },
                               { _id: 2, x: 1 },
                             ], ordered: false)
      raise('Should have raised')
    rescue Mongo::Error::BulkWriteError => e
      e.message.should =~ /\AMultiple errors: \[11000\]: (insertDocument :: caused by :: 11000 )?E11000 duplicate key error (collection|index):.*; \[121\]: Document failed validation( -- .*)?/
      # The duplicate key error should not print details because it's not a
      # WriteError or a WriteConcernError
      e.message.scan(' -- ').length.should be <= 1
    end
  end

  context 'when include_server_address_in_errors is enabled' do
    around do |example|
      original = Mongo.include_server_address_in_errors
      Mongo.include_server_address_in_errors = true
      example.run
    ensure
      Mongo.include_server_address_in_errors = original
    end

    it 'includes the server address suffix in the bulk error message' do
      collection.insert_one(_id: 1)
      begin
        collection.insert_many([ { _id: 1 }, { _id: 2 }, { _id: 2 } ])
      rescue Mongo::Error::BulkWriteError => e
        expect(e.message).to match(/\(on [^)]+:\d+(?:, [^)]+:\d+)*\)\z/)
        expect(e.server_addresses).not_to be_empty
      else
        raise 'expected BulkWriteError'
      end
    end
  end
end
