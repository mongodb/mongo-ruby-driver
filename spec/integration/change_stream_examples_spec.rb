# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'change streams examples in Ruby' do
  min_server_fcv '3.6'
  require_topology :replica_set
  require_wired_tiger

  # On JRuby, change streams should be accessed using try_next on the
  # change stream objects rather than using the Enumerable interface.
  # https://jira.mongodb.org/browse/RUBY-1877
  fails_on_jruby

  let!(:inventory) do
    client[:inventory]
  end

  let(:client) do
    authorized_client.with(max_pool_size: 5, wait_queue_timeout: 3)
  end

  before do
    inventory.drop
  end

  context 'example 1 - basic watching'do

    it 'returns a change after an insertion' do

      insert_thread = Thread.new do
        sleep 2
        inventory.insert_one(x: 1)
      end

      stream_thread = Thread.new do

        # Start Changestream Example 1

        cursor = inventory.watch.to_enum
        next_change = cursor.next

        # End Changestream Example 1
      end

      insert_thread.value
      change = stream_thread.value

      expect(change['_id']).not_to be_nil
      expect(change['_id']['_data']).not_to be_nil
      expect(change['operationType']).to eq('insert')
      expect(change['fullDocument']).not_to be_nil
      expect(change['fullDocument']['_id']).not_to be_nil
      expect(change['fullDocument']['x']).to eq(1)
      expect(change['ns']).not_to be_nil
      expect(change['ns']['db']).to eq(SpecConfig.instance.test_db)
      expect(change['ns']['coll']).to eq(inventory.name)
      expect(change['documentKey']).not_to be_nil
      expect(change['documentKey']['_id']).to eq(change['fullDocument']['_id'])
    end
  end

  context 'example 2 - full document update lookup specified' do

    it 'returns a change and the delta after an insertion' do

      inventory.insert_one(_id: 1, x: 2)

      update_thread = Thread.new do
        sleep 2
        inventory.update_one({ _id: 1}, { '$set' => { x: 5 }})
      end

      stream_thread = Thread.new do

        # Start Changestream Example 2

        cursor = inventory.watch([], full_document: 'updateLookup').to_enum
        next_change = cursor.next

        # End Changestream Example 2
      end


      update_thread.value
      change = stream_thread.value

      expect(change['_id']).not_to be_nil
      expect(change['_id']['_data']).not_to be_nil
      expect(change['operationType']).to eq('update')
      expect(change['fullDocument']).not_to be_nil
      expect(change['fullDocument']['_id']).to eq(1)
      expect(change['fullDocument']['x']).to eq(5)
      expect(change['ns']).not_to be_nil
      expect(change['ns']['db']).to eq(SpecConfig.instance.test_db)
      expect(change['ns']['coll']).to eq(inventory.name)
      expect(change['documentKey']).not_to be_nil
      expect(change['documentKey']['_id']).to eq(1)
      expect(change['updateDescription']).not_to be_nil
      expect(change['updateDescription']['updatedFields']).not_to be_nil
      expect(change['updateDescription']['updatedFields']['x']).to eq(5)
      expect(change['updateDescription']['removedFields']).to eq([])
    end
  end

  context 'example 3 - resuming from a previous change' do

    it 'returns the correct change when resuming' do

      insert_thread = Thread.new do
        sleep 2
        inventory.insert_one(x: 1)
        inventory.insert_one(x: 2)
      end

      next_change = nil
      resume_stream_thread = Thread.new do

        # Start Changestream Example 3

        change_stream = inventory.watch
        cursor = change_stream.to_enum
        next_change = cursor.next
        resume_token = change_stream.resume_token

        new_cursor = inventory.watch([], resume_after: resume_token).to_enum
        resumed_change = new_cursor.next

        # End Changestream Example 3
      end

      insert_thread.value
      resumed_change = resume_stream_thread.value

      expect(next_change['_id']).not_to be_nil
      expect(next_change['_id']['_data']).not_to be_nil
      expect(next_change['operationType']).to eq('insert')
      expect(next_change['fullDocument']).not_to be_nil
      expect(next_change['fullDocument']['_id']).not_to be_nil
      expect(next_change['fullDocument']['x']).to eq(1)
      expect(next_change['ns']).not_to be_nil
      expect(next_change['ns']['db']).to eq(SpecConfig.instance.test_db)
      expect(next_change['ns']['coll']).to eq(inventory.name)
      expect(next_change['documentKey']).not_to be_nil
      expect(next_change['documentKey']['_id']).to eq(next_change['fullDocument']['_id'])

      expect(resumed_change['_id']).not_to be_nil
      expect(resumed_change['_id']['_data']).not_to be_nil
      expect(resumed_change['operationType']).to eq('insert')
      expect(resumed_change['fullDocument']).not_to be_nil
      expect(resumed_change['fullDocument']['_id']).not_to be_nil
      expect(resumed_change['fullDocument']['x']).to eq(2)
      expect(resumed_change['ns']).not_to be_nil
      expect(resumed_change['ns']['db']).to eq(SpecConfig.instance.test_db)
      expect(resumed_change['ns']['coll']).to eq(inventory.name)
      expect(resumed_change['documentKey']).not_to be_nil
      expect(resumed_change['documentKey']['_id']).to eq(resumed_change['fullDocument']['_id'])

      expect(resumed_change.length).to eq(resumed_change.length)
      resumed_change.each { |key| expect(resumed_change[key]).to eq(resumed_change[key]) }
    end
  end

  context 'example 4 - using a pipeline to filter changes' do

    it 'returns the filtered changes' do

      ops_thread = Thread.new do
        sleep 2
        inventory.insert_one(username: 'wallace')
        inventory.insert_one(username: 'alice')
        inventory.delete_one(username: 'wallace')
      end

      stream_thread = Thread.new do

        # Start Changestream Example 4

        pipeline = [
          { "$match" => { 'fullDocument.username' => 'alice' } },
          { "$addFields" => { 'newField' => 'this is an added field!' } }
        ];
        cursor = inventory.watch(pipeline).to_enum
        cursor.next

        # End Changestream Example 4
      end

      ops_thread.value
      change = stream_thread.value

      expect(change['_id']).not_to be_nil
      expect(change['_id']['_data']).not_to be_nil
      expect(change['operationType']).to eq('insert')
      expect(change['fullDocument']).not_to be_nil
      expect(change['fullDocument']['_id']).not_to be_nil
      expect(change['fullDocument']['username']).to eq('alice')
      expect(change['newField']).not_to be_nil
      expect(change['newField']).to eq('this is an added field!')
      expect(change['ns']).not_to be_nil
      expect(change['ns']['db']).to eq(SpecConfig.instance.test_db)
      expect(change['ns']['coll']).to eq(inventory.name)
      expect(change['documentKey']).not_to be_nil
      expect(change['documentKey']['_id']).to eq(change['fullDocument']['_id'])
    end
  end
end
