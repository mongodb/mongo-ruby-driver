require 'spec_helper'

describe 'OperationFailure code' do
  let(:collection_name) { 'operation_error_code_spec' }
  let(:collection) { authorized_client[collection_name] }

  before do
    collection.delete_many
  end

  context 'duplicate key error' do
    it 'is set' do
      begin
        collection.insert_one(_id: 1)
        collection.insert_one(_id: 1)
        fail('Should have raised')
      rescue Mongo::Error::OperationFailure => e
        expect(e.code).to eq(11000)
        # there is no code name here
        expect(e.code_name).to be nil
      end
    end
  end
end
