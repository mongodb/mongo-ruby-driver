# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'OperationFailure code' do
  let(:collection_name) { 'operation_failure_code_spec' }
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
        # 4.0 and 4.2 sharded clusters set code name.
        # 4.0 and 4.2 replica sets and standalones do not,
        # and neither do older versions.
        expect([nil, 'DuplicateKey']).to include(e.code_name)
      end
    end
  end
end
