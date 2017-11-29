require 'spec_helper'

describe 'change streams examples in Ruby' do
  let(:client) do
    Mongo::Client.new([ '127.0.0.1:27017' ], :database => 'ruby-driver')
  end

  let (:inventory) do
    client[:inventory]
  end

  before do
    client[:inventory].drop
  end

  after do
    client[:inventory].drop
  end

  context 'example 1 - basic watching', if: test_change_streams? do
    it 'returns a change after an insertion' do
      Thread.new do
        sleep 1
        client[:inventory].insert_one(x: 1)
      end

      # Start Changestream Example 1

      cursor = client[:inventory].watch.to_enum
      next_change = cursor.next

      # End Changestream Example 1

      expect(next_change["_id"]).not_to be_nil
      expect(next_change["_id"]["_data"]).not_to be_nil
      expect(next_change["operationType"]).to eq("insert")
      expect(next_change["fullDocument"]).not_to be_nil
      expect(next_change["fullDocument"]["_id"]).not_to be_nil
      expect(next_change["fullDocument"]["x"]).to eq(1)
      expect(next_change["ns"]).not_to be_nil
      expect(next_change["ns"]["db"]).to eq("ruby-driver")
      expect(next_change["ns"]["coll"]).to eq("inventory")
      expect(next_change["documentKey"]).not_to be_nil
      expect(next_change["documentKey"]["_id"]).to eq(next_change["fullDocument"]["_id"])
    end
  end

  context 'example 2 - full document update lookup specified', if: test_change_streams? do
    it 'returns a change and the delta after an insertion' do
      inventory.insert_one(_id: 1, x: 2)

      Thread.new do
        sleep 1
        inventory.update_one({ _id: 1}, { "$set" => { x: 5 }})
      end

      # Start Changestream Example 2

      cursor = inventory.watch([], full_document: 'updateLookup').to_enum
      next_change = cursor.next

      # End Changestream Example 2

      expect(next_change["_id"]).not_to be_nil
      expect(next_change["_id"]["_data"]).not_to be_nil
      expect(next_change["operationType"]).to eq("update")
      expect(next_change["fullDocument"]).not_to be_nil
      expect(next_change["fullDocument"]["_id"]).to eq(1)
      expect(next_change["fullDocument"]["x"]).to eq(5)
      expect(next_change["ns"]).not_to be_nil
      expect(next_change["ns"]["db"]).to eq("ruby-driver")
      expect(next_change["ns"]["coll"]).to eq("inventory") 
      expect(next_change["documentKey"]).not_to be_nil
      expect(next_change["documentKey"]["_id"]).to eq(1)
      expect(next_change["updateDescription"]).not_to be_nil
      expect(next_change["updateDescription"]["updatedFields"]).not_to be_nil
      expect(next_change["updateDescription"]["updatedFields"]["x"]).to eq(5)
      expect(next_change["updateDescription"]["removedFields"]).to eq([])
    end
  end

  context 'example 3 - resuming from a previous change', if: test_change_streams? do
    it 'returns the correct change when resuming' do
      Thread.new do
        sleep 1
        inventory.insert_one(x: 1)
      end

      cursor = inventory.watch.to_enum
      next_change = cursor.next

      expect(next_change["_id"]).not_to be_nil
      expect(next_change["_id"]["_data"]).not_to be_nil
      expect(next_change["operationType"]).to eq("insert")
      expect(next_change["fullDocument"]).not_to be_nil
      expect(next_change["fullDocument"]["_id"]).not_to be_nil
      expect(next_change["fullDocument"]["x"]).to eq(1)
      expect(next_change["ns"]).not_to be_nil
      expect(next_change["ns"]["db"]).to eq("ruby-driver")
      expect(next_change["ns"]["coll"]).to eq("inventory")
      expect(next_change["documentKey"]).not_to be_nil
      expect(next_change["documentKey"]["_id"]).to eq(next_change["fullDocument"]["_id"])

      inventory.insert_one(x: 2)
      next_next_change = cursor.next

      expect(next_next_change["_id"]).not_to be_nil
      expect(next_next_change["_id"]["_data"]).not_to be_nil
      expect(next_next_change["operationType"]).to eq("insert")
      expect(next_next_change["fullDocument"]).not_to be_nil
      expect(next_next_change["fullDocument"]["_id"]).not_to be_nil
      expect(next_next_change["fullDocument"]["x"]).to eq(2)
      expect(next_next_change["ns"]).not_to be_nil
      expect(next_next_change["ns"]["db"]).to eq("ruby-driver")
      expect(next_next_change["ns"]["coll"]).to eq("inventory")
      expect(next_next_change["documentKey"]).not_to be_nil
      expect(next_next_change["documentKey"]["_id"]).to eq(next_next_change["fullDocument"]["_id"])

      # Start Changestream Example 3

      resume_token = next_change["_id"]
      cursor = inventory.watch([], resume_after: resume_token).to_enum
      resumed_change = cursor.next

      # End Changestream Example 3

      expect(resumed_change.length).to eq(next_next_change.length)
      resumed_change.each { |key| expect(resumed_change[key]).to eq(next_next_change[key]) }
    end
  end
end
