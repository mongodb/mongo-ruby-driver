# Copyright (C) 2009-2013 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'test_helper'
require 'json'

module Mongo
  class Collection
    public :batch_write
  end

  class BulkWriteCollectionView
    public :update_doc?, :replace_doc?, :nil_tally, :merge_result

    # for reference and future server direction
    def generate_batch_commands(groups, write_concern)
      groups.collect do |op_type, documents|
        {
            op_type => @collection.name,
            Mongo::CollectionWriter::WRITE_COMMAND_ARG_KEY[op_type] => documents,
            :ordered => @options[:ordered],
            :writeConcern => write_concern
        }
      end
    end
  end

  class MongoDBError
    def inspect
      "#{self.class.name}.new(#{message.inspect},#{error_code.inspect},#{result.inspect})"
    end
  end
end

module BSON
  class InvalidDocument
    def inspect
      "#{self.class.name}.new(#{message.inspect})"
    end
  end
end

class BulkWriteCollectionViewTest < Test::Unit::TestCase
  COLLECTION_NAME = 'test'
  DUPLICATE_KEY_ERROR_CODE_SET = [11000, 11001, 12582, 16460].to_set

  def assert_bulk_op_pushed(expected, view)
    assert_equal expected, view.ops.last
  end

  def assert_is_bulk_write_collection_view(view)
    assert_equal Mongo::BulkWriteCollectionView, view.class
  end

  def assert_bulk_exception(expected, message = '')
    ex = assert_raise BulkWriteError, message do
      pp yield
    end
    assert_equal(Mongo::ErrorCode::MULTIPLE_ERRORS_OCCURRED, ex.error_code, message)
    assert_match_document(expected, ex.result, message)
  end

  def default_setup
    @client = standard_connection
    @version = @client.server_version
    @db = @client[TEST_DB]
    @collection = @db[COLLECTION_NAME]
    @collection.drop
    @bulk = @collection.initialize_ordered_bulk_op
    @q = {:a => 1}
    @u = {"$inc" => {:x => 1}}
    @r = {:b => 2}
  end

  def sort_docs(docs)
    docs.sort{|a,b| [a.keys, a.values] <=> [b.keys, b.values]}
  end

  def generate_sized_doc(size)
    doc = {"_id" => BSON::ObjectId.new, "x" => "y"}
    serialize_doc = BSON::BSON_CODER.serialize(doc, false, false, size)
    doc = {"_id" => BSON::ObjectId.new, "x" => "y" * (1 + size - serialize_doc.size)}
    assert_equal size, BSON::BSON_CODER.serialize(doc, false, false, size).size
    doc
  end

  context "Bulk API Collection" do
    setup do
      default_setup
    end

    should "inspect" do
      assert_equal String, @bulk.inspect.class
    end

    should "check first key is operation for #update_doc?" do
      assert_not_nil @bulk.update_doc?({"$inc" => {:x => 1}})
      assert_false @bulk.update_doc?({})
      assert_nil @bulk.update_doc?({:x => 1})
    end

    should "check no top-level key is operation for #replace_doc?" do
      assert_true @bulk.replace_doc?({:x => 1})
      assert_true @bulk.replace_doc?({})
      assert_false @bulk.replace_doc?({"$inc" => {:x => 1}})
      assert_false @bulk.replace_doc?({:a => 1, "$inc" => {:x => 1}})
    end

    should "generate_batch_commands" do
      groups = [
          [:insert, [{:n => 0}]],
          [:update, [{:n => 1}, {:n => 2}]],
          [:delete, [{:n => 3}]],
          [:insert, [{:n => 5}, {:n => 6}, {:n => 7}]],
          [:update, [{:n => 8}]],
          [:delete, [{:n => 9}, {:n => 10}]]
      ]
      write_concern = {:w => 1}
      result = @bulk.generate_batch_commands(groups, write_concern)
      expected = [
          {:insert => COLLECTION_NAME, :documents => [{:n => 0}], :ordered => true, :writeConcern => {:w => 1}},
          {:update => COLLECTION_NAME, :updates => [{:n => 1}, {:n => 2}], :ordered => true, :writeConcern => {:w => 1}},
          {:delete => COLLECTION_NAME, :deletes => [{:n => 3}], :ordered => true, :writeConcern => {:w => 1}},
          {:insert => COLLECTION_NAME, :documents => [{:n => 5}, {:n => 6}, {:n => 7}], :ordered => true, :writeConcern => {:w => 1}},
          {:update => COLLECTION_NAME, :updates => [{:n => 8}], :ordered => true, :writeConcern => {:w => 1}},
          {:delete => COLLECTION_NAME, :deletes => [{:n => 9}, {:n => 10}], :ordered => true, :writeConcern => {:w => 1}}
      ]
      assert_equal expected, result
    end

    should "Initialize an unordered bulk op - spec Bulk Operation Builder" do
      @bulk = @collection.initialize_unordered_bulk_op
      assert_is_bulk_write_collection_view(@bulk)
      assert_equal @collection, @bulk.collection
      assert_equal false, @bulk.options[:ordered]
    end

    should "Initialize an ordered bulk op - spec Bulk Operation Builder" do
      assert_is_bulk_write_collection_view(@bulk)
      assert_equal @collection, @bulk.collection
      assert_equal true, @bulk.options[:ordered]
    end
  end

  def big_example(bulk)
    bulk.insert({:a => 1})
    bulk.insert({:a => 2})
    bulk.insert({:a => 3})
    bulk.insert({:a => 4})
    bulk.insert({:a => 5})
    # Update one document matching the selector
    bulk.find({:a => 1}).update_one({"$inc" => {:x => 1}})
    # Update all documents matching the selector
    bulk.find({:a => 2}).update({"$inc" => {:x => 2}})
    # Replace entire document (update with whole doc replace)
    bulk.find({:a => 3}).replace_one({:x => 3})
    # Update one document matching the selector or upsert
    bulk.find({:a => 1}).upsert.update_one({"$inc" => {:x => 1}})
    # Update all documents matching the selector or upsert
    bulk.find({:a => 2}).upsert.update({"$inc" => {:x => 2}})
    # Replaces a single document matching the selector or upsert
    bulk.find({:a => 3}).upsert.replace_one({:x => 3})
    # Remove a single document matching the selector
    bulk.find({:a => 4}).remove_one()
    # Remove all documents matching the selector
    bulk.find({:a => 5}).remove()
    # Insert a document
    bulk.insert({:x => 4})
  end

  def nil_tally_responses(responses, key)
    result = {}
    responses.each do |response|
      @bulk.nil_tally(result, key, response[key])
    end
    result
  end

  context "Bulk API CollectionView" do
    setup do
      default_setup
    end

    # ----- INSERT -----

    should "set :insert, :documents, terminate and return view for #insert" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        document = {:a => 5}
        result = @bulk.insert(document)
        assert_is_bulk_write_collection_view(result)
        assert_bulk_op_pushed [:insert, {:d => document}], @bulk
        result = @bulk.execute
        assert_match_document(
            {
                "ok" => 1,
                "n" => 1,
                "nInserted" => 1
            }, result, "wire_version:#{wire_version}")
        assert_equal 1, @collection.count
      end
    end

    should "error out on $-prefixed keys with #insert" do
      assert_raise BulkWriteError do
        @bulk.insert({ "$key" => 1 })
        @bulk.execute
      end
    end

    should "attempt to run #insert with find() and succeed, ignoring find()" do
      @bulk.find({}).insert({})
      @bulk.execute
    end

    # ----- FIND -----

    should "set :q and return view for #find" do
      result = @bulk.find(@q)
      assert_is_bulk_write_collection_view(result)
      assert_equal @q, @bulk.op_args[:q]
      @bulk.find({})
      assert_equal({}, @bulk.op_args[:q])
      @bulk.find(:a => 1)
      assert_equal({:a => 1}, @bulk.op_args[:q])
    end

    should "raise an exception for empty #find" do
      assert_raise MongoArgumentError do
        @bulk.find({})
        @bulk.execute
      end
    end

    # ----- UPDATE -----

    should "set :upsert for #upsert" do
      result = @bulk.find(@q).upsert
      assert_is_bulk_write_collection_view(result)
      assert_true result.op_args[:upsert]
    end

    should "check arg for update, set :update, :u, :multi, terminate and return view for #update" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        @collection.insert({:a => 1, :b => 1})
        @collection.insert({:a => 2, :b => 1})
        @collection.insert({:a => 2, :b => 1})
        bulk = @collection.initialize_ordered_bulk_op

        u = {"$inc" => {:b => 1}}
        q = {:a => 2}

        assert_raise_error(MongoArgumentError, "non-nil query must be set via find") do
          bulk.update(u)
        end
        assert_raise_error(MongoArgumentError, "document must start with an operator") do
          bulk.find(q).update(q)
        end

        result = bulk.find({:a => 2}).update(u)
        assert_is_bulk_write_collection_view(result)
        assert_bulk_op_pushed [:update, {:q => q, :u => u, :multi => true}], bulk

        result = bulk.execute
        assert_match_document(
            {
                "ok" => 1,
                "n" => 2,
                "nMatched" => 2,
                "nModified" => batch_commands?(wire_version) ? 2 : nil,
            }, result, "wire_version:#{wire_version}")
        assert_equal 1, @collection.find({:b => 1}).count
      end
    end

    # ----- UPDATE_ONE -----

    should "check arg for update, set :update, :u, :multi, terminate and return view for #update_one" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        @collection.insert({:a => 1})
        @collection.insert({:a => 1, :b => 2})
        bulk = @collection.initialize_ordered_bulk_op

        assert_raise_error(MongoArgumentError, "non-nil query must be set via find") do
          bulk.update_one(@u)
        end
        assert_raise_error(MongoArgumentError, "document must start with an operator") do
          bulk.find(@q).update_one(@r)
        end
        result = bulk.find(@q).update_one(@u)
        assert_is_bulk_write_collection_view(result)
        assert_bulk_op_pushed [:update, {:q => @q, :u => @u, :multi => false}], bulk
        result = bulk.execute
        assert_match_document(
            {
                "ok" => 1,
                "n" => 1,
                "nMatched" => 1,
                "nModified" => batch_commands?(wire_version) ? 1 : nil,
            }, result, "wire_version:#{wire_version}")
        assert_equal 2, @collection.count
      end
    end

    should "error-out in server when $-prefixed key is passed to #update_one" do
      assert_raise BulkWriteError do
        oh = BSON::OrderedHash.new
        oh["$key"] = 1
        oh[:a] = 1
        @bulk.find(@q).update(oh)
        @bulk.execute
      end
    end

    should "error-out in driver when first field passed to #update_one is not operator" do
      assert_raise_error(MongoArgumentError, "document must start with an operator") do
        oh = BSON::OrderedHash.new
        oh[:a] = 1
        oh["$key"] = 1
        @bulk.find(@q).update(oh)
      end
    end

    # ----- REPLACE -----

    should "raise an error when we attempt to use replace" do
      assert_raise NoMethodError do
        bulk = @collection.initialize_ordered_bulk_op
        bulk.find({:a => 2}).replace({:a => 1})
        bulk.execute
      end
    end

    # ----- REPLACE_ONE -----

    should "check arg for replacement, set :update, :u, :multi, terminate and return view for #replace_one" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        @collection.insert({:a => 1})
        @collection.insert({:a => 1})
        bulk = @collection.initialize_ordered_bulk_op
        q = {:a => 1}
        r = {:a => 2}

        assert_raise_error(MongoArgumentError, "non-nil query must be set via find") do
          bulk.replace_one(q)
        end
        assert_raise_error(MongoArgumentError, "document must not contain any operators") do
          bulk.find(q).replace_one(@u)
        end

        result = bulk.find(q).replace_one(r)
        assert_is_bulk_write_collection_view(result)
        assert_bulk_op_pushed [:update, {:q => q, :u => r, :multi => false}], bulk

        result = bulk.execute
        assert_match_document(
            {
                "ok" => 1,
                "n" => 1,
                "nMatched" => 1,
                "nModified" => batch_commands?(wire_version) ? 1 : nil,
            }, result, "wire_version:#{wire_version}")
        assert_equal 1, @collection.find(q).count
      end
    end

    # ----- REMOVE -----

    should "remove all documents when empty selector is passed to #remove" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.insert({:a => 1})
        @collection.insert({:a => 2})
        @bulk.find({}).remove
        result = @bulk.execute
        assert_equal 0, @collection.count
      end
    end

    should "#remove only documents that match selector" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        @collection.insert({:a => 1})
        @collection.insert({:a => 2})
        @bulk.find({:a => 1}).remove
        result = @bulk.execute
        assert_equal 1, @collection.count
        # should fail if we re-execute
        assert_raise_error(MongoArgumentError, "batch is empty") do
          @bulk.execute
        end
      end
    end

    should "set :remove, :q, :limit, terminate and return view for #remove" do
      assert_raise_error(MongoArgumentError, "non-nil query must be set via find") do
        @bulk.remove
      end
      result = @bulk.find(@q).remove
      assert_is_bulk_write_collection_view(result)
      assert_bulk_op_pushed [:delete, {:q => @q, :limit => 0}], @bulk
    end

    # ----- REMOVE_ONE -----

    should "set :remove, :q, :limit, terminate and return view for #remove_one" do
      assert_raise_error(MongoArgumentError, "non-nil query must be set via find") do
        @bulk.remove_one
      end
      result = @bulk.find(@q).remove_one
      assert_is_bulk_write_collection_view(result)
      assert_bulk_op_pushed [:delete, {:q => @q, :limit => 1}], @bulk
    end

    should "remove only one of several matching documents for #remove_one" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        @collection.insert({:a => 1, :b => 1})
        @collection.insert({:a => 1, :b => 2})
        @bulk.find({:a => 1}).remove_one
        result = @bulk.execute
        assert_match_document(
            {
                "ok" => 1,
                "n" => 1,
                "nRemoved" => 1,
                "nModified" => nil,
            }, result, "wire_version:#{wire_version}")
        assert_equal 1, @collection.count
      end
    end

    # ----- UPSERT-UPDATE -----

    should "handle single upsert - spec Handling upserts" do # chose array always for upserted value
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        @collection.ensure_index(BSON::OrderedHash[:a, Mongo::ASCENDING], {:unique => true})
        bulk = @collection.initialize_ordered_bulk_op

        assert_raise_error(MongoArgumentError, "non-nil query must be set via find") do
          @bulk.upsert.update({"$set" => {:a => 1}})
        end

        bulk.find({:a => 1}).upsert.update({'$set' => {:a => 2}})
        result = bulk.execute
        assert_match_document(
            {
                "ok" => 1,
                "n" => 1,
                "nMatched" => 0,
                "nUpserted" => 1,
                "nModified" => batch_commands?(wire_version) ? 0 : nil,
                "upserted" => [
                    {"_id" => BSON::ObjectId('52a16767bb67fbc77e26a310'), "index" => 0}
                ]
            }, result, "wire_version:#{wire_version}")
      end
    end

    should "run #upsert.update without affecting non-upsert updates" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove

        bulk = @collection.initialize_unordered_bulk_op
        bulk.find({:a => 1}).update({"$set" => {:x => 1}})
        bulk.find({:a => 2}).upsert.update({"$set" => {:x => 2}})
        result = bulk.execute
        assert_match_document(
            {
                "ok" => 1,
                "n" => 1,
                "nMatched" => 0,
                "nModified" => batch_commands?(wire_version) ? 0 : nil,
                "nUpserted" => 1,
                "upserted" => [
                    {"_id" => BSON::ObjectId('52a16767bb67fbc77e26a310'), "index" => 1}
                ]
            }, result, "wire_version:#{wire_version}")

        # Repeat the batch and nMatched = 1, nUpserted = 0
        bulk2 = @collection.initialize_unordered_bulk_op
        bulk2.find({:a => 1}).update({"$set" => {:x => 1}})
        bulk2.find({:a => 2}).upsert.update({"$set" => {:x => 2}})
        result2 = bulk2.execute
        assert_match_document(
            {
                "ok" => 1,
                "n" => 1,
                "nMatched" => 1,
                "nModified" => batch_commands?(wire_version) ? 0 : nil
            }, result2, "wire_version:#{wire_version}")
      end
    end

    should "count nUpserted correctly when _id is not an ObjectId (upsert-update)" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove

        bulk = @collection.initialize_unordered_bulk_op
        bulk.find({:_id => 3}).upsert.update({"$set" => {:b => 3}})
        result = bulk.execute
        assert_match_document(
            {
                "ok" => 1,
                "n" => 1,
                "nMatched" => 0,
                "nUpserted" => 1,
                "nModified" => batch_commands?(wire_version) ? 0 : nil,
                "upserted" => [
                    { "_id" => 3, "index" => 0 }
                ]
            }, result, "wire_version:#{wire_version}")
      end
    end

    # ----- UPSERT-UPDATE_ONE -----

    should "#upsert a document without affecting non-upsert update_ones" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        bulk = @collection.initialize_unordered_bulk_op

        bulk.find({:a => 1}).update_one({"$set" => {:x => 1}})
        bulk.find({:a => 2}).upsert.update_one({"$set" => {:x => 2}})
        result = bulk.execute
        assert_match_document(
            {
                "ok" => 1,
                "n" => 1,
                "nMatched" => 0,
                "nUpserted" => 1,
                "nModified" => batch_commands?(wire_version) ? 0 : nil,
                "upserted" => [
                    {"_id" => BSON::ObjectId('52a16767bb67fbc77e26a310'), "index" => 1}
                ]
            }, result, "wire_version:#{wire_version}")
      end
    end

    should "only update one matching document with #upsert-update_one" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        @collection.insert({:a => 1})
        @collection.insert({:a => 1})

        bulk = @collection.initialize_unordered_bulk_op
        bulk.find({:a => 1}).update_one({"$set" => {:x => 1}})
        result = bulk.execute
        assert_match_document(
            {
                "ok" => 1,
                "n" => 1,
                "nMatched" => 1,
                "nModified" => batch_commands?(wire_version) ? 1 : nil,
            }, result, "wire_version:#{wire_version}")
      end
    end


    should "count nUpserted correctly when _id is not an ObjectId (upsert-update_one)" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        bulk = @collection.initialize_ordered_bulk_op
        bulk.find({:_id => 2}).upsert.update_one({"$set" => {:x => 2}})
        result = bulk.execute
        assert_match_document(
            {
                "ok" => 1,
                "n" => 1,
                "nMatched" => 0,
                "nUpserted" => 1,
                "nModified" => batch_commands?(wire_version) ? 0 : nil,
                "upserted" => [
                    {"_id" => 2, "index" => 0 }
                ]
            }, result, "wire_version:#{wire_version}")
      end
    end

    # ----- UPSERT-REPLACE_ONE -----

    should "not affect non-upsert replace_ones in same batch as #upsert-replace_one" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        bulk = @collection.initialize_unordered_bulk_op
        bulk.find({:a => 1}).replace_one({:x => 1})
        bulk.find({:a => 2}).upsert.replace_one({:x => 2})
        result = bulk.execute
        assert_match_document(
            {
                "ok" => 1,
                "n" => 1,
                "nMatched" => 0,
                "nUpserted" => 1,
                "nModified" => batch_commands?(wire_version) ? 0 : nil,
                "upserted" => [
                    {"_id" => BSON::ObjectId('52a16767bb67fbc77e26a310'), "index" => 1}
                ]
            }, result, "wire_version:#{wire_version}")
        assert_equal 1, @collection.count
      end
    end

    should "only replace one matching document with #upsert-replace_one" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        @collection.insert({:a => 1})
        @collection.insert({:a => 1})
        bulk = @collection.initialize_unordered_bulk_op
        bulk.find({:a => 1}).replace_one({:x => 1})
        bulk.find({:a => 2}).upsert.replace_one({:x => 2})
        result = bulk.execute
        assert_match_document(
            {
                "ok" => 1,
                "n" => 2,
                "nMatched" => 1,
                "nUpserted" => 1,
                "nModified" => batch_commands?(wire_version) ? 1 : nil,
                "upserted" => [
                    {"_id" => BSON::ObjectId('52a16767bb67fbc77e26a310'), "index" => 1}
                ]
            }, result, "wire_version:#{wire_version}")
        assert_equal 3, @collection.count
      end
    end

    should "tally given all numbers or return nil for #nil_tally" do
      assert_equal({"nM" => 6}, nil_tally_responses([{"nM" => 1}, {"nM" => 2}, {"nM" => 3}], "nM"))
      assert_equal({"nM" => nil}, nil_tally_responses([{"nM" => 1}, { }, {"nM" => 3}], "nM"))
      assert_equal({"nM" => nil}, nil_tally_responses([{"nM" => 1}, {"nM" => nil}, {"nM" => 3}], "nM"))
    end


    should "count nUpserted correctly when _id is not an ObjectId (upsert-replace_one)" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        bulk = @collection.initialize_unordered_bulk_op
        bulk.find({:a => 1}).upsert.replace_one({:_id => 2})
        result = bulk.execute
        assert_match_document(
            {
                "ok" => 1,
                "n" => 1,
                "nMatched" => 0,
                "nUpserted" => 1,
                "nModified" => batch_commands?(wire_version) ? 0 : nil,
                "upserted" => [
                                { "_id" => 2, "index" => 0 }
                ]
            }, result, "wire_version:#{wire_version}")
        assert_equal 1, @collection.count
      end
    end

    # ----- MIXED OPS, ORDERED -----

    should "execute, return result and reset @ops for #execute" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        @bulk.insert({:x => 1})
        @bulk.insert({:x => 2})
        write_concern = {:w => 1}
        result = @bulk.execute(write_concern)
        assert_equal({"ok" => 1, "n" => 2, "nInserted" => 2}, result, "wire_version:#{wire_version}")
        assert_equal 2, @collection.count
        assert_equal [], @bulk.ops
      end
    end

    should "run ordered big example" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        big_example(@bulk)
        write_concern = {:w => 1} #{:w => 1. :j => 1} #nojournal for tests
        result = @bulk.execute(write_concern)
        assert_match_document(
            {
                "ok" => 1,
                "n" => 14,
                "nInserted" => 6,
                "nMatched" => 5,
                "nUpserted" => 1,
                "nModified" => batch_commands?(wire_version) ? 5 : nil,
                "nRemoved" => 2,
                "upserted" => [
                    {
                        "index" => 10,
                        "_id" => BSON::ObjectId('52a1e4a4bb67fbc77e26a340')
                    }
                ]
            }, result, "wire_version:#{wire_version}")
        assert_equal(batch_commands?(wire_version), result.has_key?("nModified"), "wire_version:#{wire_version}")
        assert_false(@collection.find.to_a.empty?, "wire_version:#{wire_version}")
        assert_equal [{"a"=>1, "x"=>2}, {"a"=>2, "x"=>4}, {"x"=>3}, {"x"=>3}, {"x"=>4}], sort_docs(@collection.find.to_a.collect { |doc| doc.delete("_id"); doc })
      end
    end

    should "run spec Ordered Bulk Operations" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @bulk.insert({:a => 1})
        @bulk.insert({:a => 2})
        @bulk.insert({:a => 3})
        @bulk.find({:a => 2}).upsert.update({'$set' => {:a => 4}})
        @bulk.find({:a => 1}).remove_one
        @bulk.insert({:a => 5})
        result = @bulk.execute({:w => 1})
        assert_match_document(
            {
                "ok" => 1,
                "n" => 6,
                "nInserted" => 4,
                "nMatched" => 1,
                "nModified" => batch_commands?(wire_version) ? 1 : nil,
                "nRemoved" => 1,
            }, result, "wire_version:#{wire_version}")
      end
    end

    # ----- MIXED OPS, UNORDERED -----

    should "run unordered big example" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        @bulk = @collection.initialize_unordered_bulk_op
        big_example(@bulk)
        write_concern = {:w => 1} #{:w => 1. :j => 1} #nojournal for tests
        result = @bulk.execute(write_concern)
        assert_equal(6, result["nInserted"])
        assert_true(result["n"] > 0, "wire_version:#{wire_version}")
        assert_equal(batch_commands?(wire_version), result.has_key?("nModified"), "wire_version:#{wire_version}")
        assert_false(@collection.find.to_a.empty?, "wire_version:#{wire_version}")
      end
    end

    should "run unordered big example with w 0" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        @bulk = @collection.initialize_unordered_bulk_op
        big_example(@bulk)
        write_concern = {:w => 0}
        result = @bulk.execute(write_concern)
        assert_equal(true, result, "wire_version:#{wire_version}")
        assert_false(@collection.find.to_a.empty?, "wire_version:#{wire_version}")
      end
    end

    should "run unordered bulk operations in one batch per write-type" do
      with_write_commands(@db.connection) do
        @collection.expects(:batch_write).at_most(3).returns([[], [], [], []])
        bulk = @collection.initialize_unordered_bulk_op
        bulk.insert({:_id => 1, :a => 1})
        bulk.find({:_id => 1, :a => 1}).update({"$inc" => {:x => 1}})
        bulk.find({:_id => 1, :a => 1}).remove
        bulk.insert({:_id => 2, :a => 2})
        bulk.find({:_id => 2, :a => 2}).update({"$inc" => {:x => 2}})
        bulk.find({:_id => 2, :a => 2}).remove
        bulk.insert({:_id => 3, :a => 3})
        bulk.find({:_id => 3, :a => 3}).update({"$inc" => {:x => 3}})
        bulk.find({:_id => 3, :a => 3}).remove
        result = bulk.execute # unordered varies, don't use assert_match_document
      end
    end

    should "run spec Unordered Bulk Operations" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        bulk = @collection.initialize_unordered_bulk_op
        bulk.insert({:_id => 1})
        bulk.find({:_id => 2}).update_one({'$inc' => { :x => 1 }})
        bulk.find({:_id => 3}).remove_one
        bulk.insert({:_id => 4})
        bulk.find({:_id => 5}).update_one({'$inc' => { :x => 1 }})
        bulk.find({:_id => 6}).remove_one
        result = nil
        begin
          result = bulk.execute
        rescue => ex
          result = ex.result
        end
        # for write commands internally the driver will execute 3. One each for the inserts, updates and removes.
      end
    end

    # ----- EMPTY BATCH -----

    should "handle empty bulk op" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        assert_raise_error(MongoArgumentError, Mongo::BulkWriteCollectionView::EMPTY_BATCH_MSG) do
          @bulk.execute
        end
      end
    end

    should "handle insert of overly large document" do
      large_doc = {"a" => "y"*(2*@client.max_message_size)}
      with_write_commands_and_operations(@db.connection) do |wire_version|
        ex = assert_raise Mongo::BulkWriteError do
          @collection.remove
          bulk = @collection.initialize_unordered_bulk_op
          bulk.insert(large_doc)
          puts "bulk.execute:#{bulk.execute.inspect}"
        end
        assert_equal 22, ex.result["writeErrors"].first["code"]
      end
    end

    # ----- ORDERED, WITH ERRORS -----

    should "handle error for duplicate key with offset" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        @bulk.find({:a => 1}).update_one({"$inc" => {:x => 1}})
        @bulk.insert({:_id => 1, :a => 1})
        @bulk.insert({:_id => 1, :a => 2})
        @bulk.insert({:_id => 3, :a => 3})
        ex = assert_raise BulkWriteError do
          @bulk.execute
        end
        result = ex.result
        assert_match_document(
            {
                "ok" => 1,
                "n" => 1,
                "writeErrors" =>
                    [{
                      "index" => 2,
                      "code" => 11000,
                      "errmsg" => /duplicate key error/
                    }],
                "code" => 65,
                "errmsg" => "batch item errors occurred",
                "nInserted" => 1,
                "nMatched" => 0,
                "nModified" => batch_commands?(wire_version) ? 0 : nil
            }, result, "wire_version:#{wire_version}")
      end
    end

    should "handle error for serialization with offset" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        assert_equal 16777216, @client.max_bson_size
        @bulk.find({:a => 1}).update_one({"$inc" => {:x => 1}})
        @bulk.insert({:_id => 1, :a => 1})
        @bulk.insert(generate_sized_doc(@client.max_message_size + 1))
        @bulk.insert({:_id => 3, :a => 3})
        ex = assert_raise BulkWriteError do
          @bulk.execute
        end
        result = ex.result
        assert_match_document(
            {
                "ok" => 1,
                "n" => 1,
                "writeErrors" =>
                    [{
                         "index" => 2,
                         "code" => 22,
                         "errmsg" => /too large/
                     }],
                "code" => 65,
                "errmsg" => "batch item errors occurred",
                "nInserted" => 1,
                "nMatched" => 0,
                "nModified" => batch_commands?(wire_version) ? 0 : nil
            }, result, "wire_version:#{wire_version}")
      end
    end

    should "run ordered bulk op - spec Modes of Execution" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        @collection.ensure_index(BSON::OrderedHash[:a, Mongo::ASCENDING], {:unique => true})
        @bulk.insert({:a => 1})
        @bulk.insert({:a => 2})
        @bulk.find({:a => 2}).update({'$set' => {:a => 1}}) # Clashes with unique index
        @bulk.find({:a => 1}).remove
        ex = assert_raise BulkWriteError do
          @bulk.execute
        end
        assert_equal(2, @collection.count)
      end
    end

    should "handle duplicate key error - spec Merging Results" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        @collection.ensure_index(BSON::OrderedHash[:a, Mongo::ASCENDING], {:unique => true})
        bulk = @collection.initialize_ordered_bulk_op
        bulk.insert({:a => 1})
        bulk.insert({:a => 2})
        bulk.find({:a => 2}).upsert.update({'$set' => {:a => 1}})
        bulk.insert({:a => 3})
        ex = assert_raise BulkWriteError do
          bulk.execute
        end
        result = ex.result
        assert_match_document(
            {
                "ok" => 1,
                "n" => 2,
                "writeErrors" =>
                    [{
                         "index" => 2,
                         "code" => DUPLICATE_KEY_ERROR_CODE_SET,
                         "errmsg" => /duplicate key error/
                     }],
                "code" => 65,
                "errmsg" => "batch item errors occurred",
                "nInserted" => 2,
                "nMatched" => 0,
                "nModified" => batch_commands?(wire_version) ? 0 : nil
            }, result, "wire_version:#{wire_version}")
      end
    end

    should "report user index - spec Merging errors" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        @collection.ensure_index(BSON::OrderedHash[:a, Mongo::ASCENDING], {:unique => true})
        bulk = @collection.initialize_ordered_bulk_op
        bulk.insert({:a => 1})
        bulk.insert({:a => 2})
        bulk.find({:a => 2}).update_one({'$set' => {:a => 1}});
        bulk.find({:a => 4}).remove_one();
        ex = assert_raise BulkWriteError do
          bulk.execute({:w => 1})
        end
        result = ex.result
        assert_match_document(
            {
                "ok" => 1,
                "n" => 2,
                "writeErrors" =>
                    [{
                         "index" => 2,
                         "code" => DUPLICATE_KEY_ERROR_CODE_SET,
                         "errmsg" => /duplicate key error/
                     }],
                "code" => 65,
                "errmsg" => "batch item errors occurred",
                "nInserted" => 2,
                "nMatched" => 0,
                "nModified" => batch_commands?(wire_version) ? 0 : nil
            }, result, "wire_version:#{wire_version}")
      end
    end

    should "handle multiple upsert - spec Handling upserts" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        @collection.ensure_index(BSON::OrderedHash[:a, Mongo::ASCENDING], {:unique => true})
        bulk = @collection.initialize_ordered_bulk_op
        bulk.find({:a => 1}).upsert.update({'$set' => {:a => 2}})
        bulk.find({:a => 3}).upsert.update({'$set' => {:a => 4}})
        result = bulk.execute
        assert_match_document(
            {
                "ok" => 1,
                "n" => 2,
                "nMatched" => 0,
                "nUpserted" => 2,
                "nModified" => batch_commands?(wire_version) ? 0 : nil,
                "upserted" => [
                    {"index" => 0, "_id" => BSON::ObjectId('52a1e37cbb67fbc77e26a338')},
                    {"index" => 1, "_id" => BSON::ObjectId('52a1e37cbb67fbc77e26a339')}
                ]
            }, result, "wire_version:#{wire_version}")
      end
    end

    should "handle replication usage error" do
      with_no_replication(@db.connection) do
        with_write_commands_and_operations(@db.connection) do |wire_version|
          @collection.remove
          @bulk = @collection.initialize_ordered_bulk_op
          @bulk.insert({:_id => 1, :a => 1})
          write_concern = {:w => 5}
          ex = assert_raise BulkWriteError do
            @bulk.execute(write_concern)
          end
          result = ex.result
          if @version >= "2.5.5"
            assert_match_document(
                {
                    "ok" => 0,
                    "n" => 0,
                    "code" => 65,
                    "errmsg" => "batch item errors occurred",
                    "writeErrors" => [
                        {
                            "errmsg" => "cannot use 'w' > 1 when a host is not replicated",
                            "code" => 2,
                            "index" => 0}
                    ],
                    "nInserted" => 0,
                }, result, "wire_version:#{wire_version}")
          else
            assert_match_document(
                {
                    "ok" => 1,
                    "n" => 1,
                    "code" => 65,
                    "errmsg" => "batch item errors occurred",
                    "writeConcernError" => [
                        {
                            "errmsg" => /no replication has been enabled/,
                            "code" => 64,
                            "index" => 0
                        }
                    ],
                    "nInserted" => 1,
                }, result, "wire_version:#{wire_version}")
          end
        end
      end
    end

    # ----- UNORDERED, WITH ERRORS -----

    should "handle error for unordered multiple duplicate key with offset" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        @bulk = @collection.initialize_unordered_bulk_op
        @bulk.find({:a => 1}).remove
        @bulk.insert({:_id => 1, :a => 1})
        @bulk.insert({:_id => 1, :a => 2})
        @bulk.insert({:_id => 3, :a => 3})
        @bulk.insert({:_id => 3, :a => 3})
        ex = assert_raise BulkWriteError do
          @bulk.execute
        end
        result = ex.result
        assert_true (0 < result["nInserted"] && result["nInserted"] < 3)
        assert_not_nil(result["writeErrors"], "wire_version:#{wire_version}")
      end
    end

    should "run unordered bulk op - spec Modes of Execution" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        @collection.ensure_index(BSON::OrderedHash[:a, Mongo::ASCENDING], {:unique => true})
        bulk = @collection.initialize_unordered_bulk_op
        bulk.insert({:a => 1})
        bulk.insert({:a => 2})
        bulk.find({:a => 2}).update({'$set' => {:a => 1}}) # Clashes with unique index
        bulk.find({:a => 3}).remove
        bulk.find({:a => 2}).update({'$set' => {:a => 1}}) # Clashes with unique index
        ex = assert_raise BulkWriteError do
          bulk.execute
        end
        result = ex.result
        assert(result["writeErrors"].size > 1, "wire_version:#{wire_version}")
      end
    end

    should "handle unordered errors - spec Merging Results" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        @collection.ensure_index(BSON::OrderedHash[:a, Mongo::ASCENDING], {:unique => true})
        bulk = @collection.initialize_unordered_bulk_op
        bulk.insert({:a => 1})
        bulk.find({:a => 1}).upsert.update({'$set' => {:a => 2}})
        bulk.insert({:a => 2})
        ex = assert_raise BulkWriteError do
          bulk.execute
        end
        result = ex.result # unordered varies, don't use assert_bulk_exception
        assert_equal(1, result['ok'], "wire_version:#{wire_version}")
        assert_equal(2, result['n'], "wire_version:#{wire_version}")
        err_details = result['writeErrors']
        assert_match(/duplicate key error/, err_details.first['errmsg'], "wire_version:#{wire_version}")
      end
    end

    should "handle multiple errors for unordered bulk write" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        @bulk = @collection.initialize_unordered_bulk_op
        @bulk.insert({:_id => 1, :a => 1})
        @bulk.insert({:_id => 1, :a => 2})
        @bulk.insert(generate_sized_doc(@client.max_message_size + 1))
        @bulk.insert({:_id => 3, :a => 3})
        @bulk.find({:a => 4}).upsert.replace_one({:x => 3})
        ex = assert_raise BulkWriteError do
          @bulk.execute
        end
        result = ex.result # unordered varies, don't use assert_bulk_exception
        assert_equal(1, result['ok'], "wire_version:#{wire_version}")
        assert_equal(3, result['n'], "wire_version:#{wire_version}")
        err_details = result['writeErrors']
        assert_match(/duplicate key error/, err_details.find { |e| e['code']==11000 }['errmsg'], "wire_version:#{wire_version}")
        assert_match(/too large/, err_details.find { |e| e['index']==2 }['errmsg'], "wire_version:#{wire_version}")
        assert_not_nil(result['upserted'].find { |e| e['index']==4 }, "wire_version:#{wire_version}")
      end
    end

    # ----- NO_JOURNAL -----

    should "handle journaling error" do
      with_no_journaling(@db.connection) do
        with_write_commands_and_operations(@db.connection) do |wire_version|
          @collection.remove
          @bulk = @collection.initialize_ordered_bulk_op
          @bulk.insert({:_id => 1, :a => 1})
          write_concern = {:w => 1, :j => 1}
          ex = assert_raise BulkWriteError do
            @bulk.execute(write_concern)
          end
          result = ex.result
          if @version >= "2.5.5"
            assert_match_document(
                {
                    "ok" => 0,
                    "n" => 0,
                    "writeErrors" => [
                        {
                            "code" => 2,
                            "errmsg" => "cannot use 'j' option when a host does not have journaling enabled", "index" => 0
                        }
                    ],
                    "code" => 65,
                    "errmsg" => "batch item errors occurred",
                    "nInserted" => 0
                }, result, "wire_version:#{wire_version}")
          else
            assert_match_document(
                {
                    "ok" => 1,
                    "n" => 1,
                    "writeConcernError" => [
                        {
                            "code" => 2,
                            "errmsg" => "journaling not enabled on this server",
                            "index" => 0
                        }
                    ],
                    "code" => 65,
                    "errmsg" => "batch item errors occurred",
                    "nInserted" => 1
                }, result, "wire_version:#{wire_version}")
          end
        end
      end
    end

    # ----- W = 0 -----

    should "run ordered big example with w 0" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        big_example(@bulk)
        result = @bulk.execute({:w => 0})
        assert_equal(true, result, "wire_version:#{wire_version}")
        assert_false(@collection.find.to_a.empty?, "wire_version:#{wire_version}")
        assert_equal [{"a"=>1, "x"=>2}, {"a"=>2, "x"=>4}, {"x"=>3}, {"x"=>3}, {"x"=>4}], sort_docs(@collection.find.to_a.collect { |doc| doc.delete("_id"); doc })
      end
    end

    should "running with w 0 should not report write errors" do
      with_write_commands_and_operations(@db.connection) do
        @bulk.insert({:_id => 1, :a => 1 })
        @bulk.insert({:_id => 1, :a => 2 })
        @bulk.execute({:w => 0}) # should raise no duplicate key error
      end
    end

    # ----- W > 0 WITH STANDALONE -----

    should "disallow w > 0 against a standalone" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        @bulk.insert({:_id => 1, :a => 1 })
        @bulk.insert({:_id => 2, :a => 1 })
        @bulk.insert({:_id => 3, :a => 1 })
        assert_raise_error BulkWriteError do
          @bulk.execute({:w => 2})
        end
        assert (@collection.count == batch_commands?(wire_version) ? 0 : 1)
      end
    end

  end
end
