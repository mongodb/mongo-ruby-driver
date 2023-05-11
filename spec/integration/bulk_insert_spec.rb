# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'Bulk insert' do
  include PrimarySocket

  let(:fail_point_base_command) do
    { 'configureFailPoint' => "failCommand" }
  end

  let(:collection_name) { 'bulk_insert_spec' }
  let(:collection) { authorized_client[collection_name] }

  describe 'inserted_ids' do
    before do
      collection.delete_many
    end

    context 'success' do
      it 'returns one insert_id as array' do
        result = collection.insert_many([
          {:_id => 9},
        ])
        expect(result.inserted_ids).to eql([9])
      end
    end

    context 'error on first insert' do
      it 'is an empty array' do
        collection.insert_one(:_id => 9)
        begin
          result = collection.insert_many([
            {:_id => 9},
          ])
          fail 'Should have raised'
        rescue Mongo::Error::BulkWriteError => e
          expect(e.result['inserted_ids']).to eql([])
        end
      end
    end

    context 'error on third insert' do
      it 'is an array of the first two ids' do
        collection.insert_one(:_id => 9)
        begin
          result = collection.insert_many([
            {:_id => 7},
            {:_id => 8},
            {:_id => 9},
          ])
          fail 'Should have raised'
        rescue Mongo::Error::BulkWriteError => e
          expect(e.result['inserted_ids']).to eql([7, 8])
        end
      end
    end

    context 'entire operation fails' do
      min_server_fcv '4.0'
      require_topology :single, :replica_set

      it 'is an empty array' do
        collection.client.use(:admin).command(fail_point_base_command.merge(
          :mode => {:times => 1},
          :data => {:failCommands => ['insert'], errorCode: 100}))
        begin
          result = collection.insert_many([
            {:_id => 7},
            {:_id => 8},
            {:_id => 9},
          ])
          fail 'Should have raised'
        rescue Mongo::Error => e
          result = e.send(:instance_variable_get, '@result')
          expect(result).to be_a(Mongo::Operation::Insert::BulkResult)
          expect(result.inserted_ids).to eql([])
        end
      end
    end
  end
end
