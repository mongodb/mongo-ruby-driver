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

class Hash
  def stringify_keys
    dup.stringify_keys!
  end

  def stringify_keys!
    keys.each do |key|
      self[key.to_s] = delete(key)
    end
    self
  end

  def except(*keys)
    dup.except!(*keys)
  end

  # Replaces the hash without the given keys.
  def except!(*keys)
    keys.each { |key| delete(key) }
    self
  end
end

module Mongo
  class Collection
    public :batch_write
  end
  class BulkWriteCollectionView
    public :update_doc?, :replace_doc?

    # for reference and future server direction
    def generate_batch_commands(groups, write_concern)
      groups.collect do |op, documents|
        {
            op => @collection.name,
            Mongo::CollectionWriter::WRITE_COMMAND_ARG_KEY[op] => documents,
            :ordered => @options[:ordered],
            :writeConcern => write_concern
        }
      end
    end
  end
end

class BulkWriteCollectionViewTest < Test::Unit::TestCase
  @@client       ||= standard_connection(:op_timeout => 10)
  @@db           = @@client.db(TEST_DB)
  @@test         = @@db.collection("test")
  @@version      = @@client.server_version

  DATABASE_NAME = 'bulk_write_collection_view_test'
  COLLECTION_NAME = 'test'

  def assert_bulk_op_pushed(expected, view)
    assert_equal expected, view.ops.last
  end

  def assert_is_bulk_write_collection_view(view)
    assert_equal Mongo::BulkWriteCollectionView, view.class
  end

  def clone_out_object_id(doc, merge = {})
    # note: Ruby 1.8.7 doesn't support \h
    JSON.parse(doc.merge(merge).to_json.gsub(/\"\$oid\": *\"[a-f0-9]{24}\"/,"\"$oid\":\"123456789012345678901234\""))
  end

  def assert_equal_json(expected, actual, merge = {}, message = nil)
    assert_equal(clone_out_object_id(expected, merge), clone_out_object_id(actual), message)
  end

  def assert_bulk_exception(result, merge = {}, message = nil)
    ex = assert_raise BulkWriteError do
      yield
    end
    assert_equal(Mongo::BulkWriteCollectionView::MULTIPLE_ERRORS_OCCURRED, ex.error_code, message)
    assert_equal_json(result, ex.result, merge, message)
  end

  def default_setup
    @client = MongoClient.new
    @db = @client[DATABASE_NAME]
    @collection = @db[COLLECTION_NAME]
    @collection.drop
    @bulk = @collection.initialize_ordered_bulk_op
    @q = {:a => 1}
    @u = {"$inc" => { :x => 1 }}
    @r = {:b => 2}
  end

  def generate_sized_doc(size)
    doc = {"_id" => BSON::ObjectId.new, "x" => "y"}
    serialize_doc = BSON::BSON_CODER.serialize(doc, false, false, size)
    doc = {"_id" => BSON::ObjectId.new, "x" => "y" * (1 + size - serialize_doc.size)}
    assert_equal size, BSON::BSON_CODER.serialize(doc, false, false, size).size
    doc
  end

  context "Bulk API Spec Collection" do
    setup do
      default_setup
    end

    should "inspect" do
      assert_equal String, @bulk.inspect.class
    end

    should "check first key is operation for #update_doc?" do
      assert_not_nil @bulk.update_doc?({"$inc" => { :x => 1 }})
      assert_false @bulk.update_doc?({})
      assert_nil @bulk.update_doc?({ :x => 1 })
    end

    should "check no top-level key is operation for #replace_doc?" do
      assert_true @bulk.replace_doc?({ :x => 1 })
      assert_true @bulk.replace_doc?({})
      assert_false @bulk.replace_doc?({"$inc" => { :x => 1 }})
      assert_false @bulk.replace_doc?({ :a => 1, "$inc" => { :x => 1 }})
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
      write_concern = {:w => 1, :j => 1}
      result = @bulk.generate_batch_commands(groups, write_concern)
      expected = [
          {:insert => COLLECTION_NAME, :documents => [{:n => 0}], :ordered => true, :writeConcern => {:j => 1, :w => 1}},
          {:update => COLLECTION_NAME, :updates => [{:n => 1}, {:n => 2}], :ordered => true, :writeConcern => {:j => 1, :w => 1}},
          {:delete => COLLECTION_NAME, :deletes => [{:n => 3}], :ordered => true, :writeConcern => {:j => 1, :w => 1}},
          {:insert => COLLECTION_NAME, :documents => [{:n => 5}, {:n => 6}, {:n => 7}], :ordered => true, :writeConcern => {:j => 1, :w => 1}},
          {:update => COLLECTION_NAME, :updates => [{:n => 8}], :ordered => true, :writeConcern => {:j => 1, :w => 1}},
          {:delete => COLLECTION_NAME, :deletes => [{:n => 9}, {:n => 10}], :ordered => true, :writeConcern => {:j => 1, :w => 1}}
      ]
      assert_equal expected, result
    end

    should "return view and set @collection and options for #initialize_ordered_bulk_op" do
      assert_is_bulk_write_collection_view(@bulk)
      assert_equal @collection, @bulk.collection
      assert_equal true, @bulk.options[:ordered]
    end

    should "return view and set @collection and options for #initialize_unordered_bulk_op" do
      @bulk = @collection.initialize_unordered_bulk_op
      assert_is_bulk_write_collection_view(@bulk)
      assert_equal @collection, @bulk.collection
      assert_equal false, @bulk.options[:ordered]
    end
  end

  def big_example(bulk)
    bulk.insert({ :a => 1 })
    bulk.insert({ :a => 2 })
    bulk.insert({ :a => 3 })
    bulk.insert({ :a => 4 })
    bulk.insert({ :a => 5 })
    # Update one document matching the selector
    bulk.find({:a => 1}).update_one({"$inc" => { :x => 1 }})
    # Update all documents matching the selector
    bulk.find({:a => 2}).update({"$inc" => { :x => 2 }})
    # Replace entire document (update with whole doc replace)
    bulk.find({:a => 3}).replace_one({ :x => 3 })
    # Update one document matching the selector or upsert
    bulk.find({:a => 1}).upsert.update_one({"$inc" => { :x => 1 }})
    # Update all documents matching the selector or upsert
    bulk.find({:a => 2}).upsert.update({"$inc" => { :x => 2 }})
    # Replaces a single document matching the selector or upsert
    bulk.find({:a => 3}).upsert.replace_one({ :x => 3 })
    # Remove a single document matching the selector
    bulk.find({:a => 4}).remove_one()
    # Remove all documents matching the selector
    bulk.find({:a => 5}).remove()
    # Insert a document
    bulk.insert({ :x => 4 })
  end

  context "Bulk API Spec CollectionView" do
    setup do
      default_setup
    end

    should "set :q and return view for #find" do
      result = @bulk.find(@q)
      assert_is_bulk_write_collection_view(result)
      assert_equal @q, @bulk.op_args[:q]
    end

    should "set :upsert for #upsert" do
      result = @bulk.find(@q).upsert
      assert_is_bulk_write_collection_view(result)
      assert_true result.op_args[:upsert]
    end

    should "check arg for update, set :update, :u, :multi, terminate and return view for #update_one" do
      assert_raise MongoArgumentError do
        @bulk.find(@q).update_one(@r)
      end
      result = @bulk.find(@q).update_one(@u)
      assert_is_bulk_write_collection_view(result)
      assert_bulk_op_pushed [:update, {:q => @q, :u => @u, :multi => false}], @bulk
    end

    should "check arg for update, set :update, :u, :multi, terminate and return view for #update" do
      assert_raise MongoArgumentError do
        @bulk.find(@q).replace_one(@u)
      end
      result = @bulk.find(@q).update(@u)
      assert_is_bulk_write_collection_view(result)
      assert_bulk_op_pushed [:update, {:q => @q, :u => @u, :multi => true}], @bulk
    end

    should "check arg for replacement, set :update, :u, :multi, terminate and return view for #replace_one" do
      assert_raise MongoArgumentError do
        @bulk.find(@q).replace_one(@u)
      end
      result = @bulk.find(@q).replace_one(@r)
      assert_is_bulk_write_collection_view(result)
      assert_bulk_op_pushed [:update, {:q => @q, :u => @r, :multi => false}], @bulk
    end

    should "set :remove, :q, :limit, terminate and return view for #remove_one" do
      result = @bulk.find(@q).remove_one
      assert_is_bulk_write_collection_view(result)
      assert_bulk_op_pushed [:delete, {:q => @q, :limit => 1}], @bulk

    end

    should "set :remove, :q, :limit, terminate and return view for #remove" do
      result = @bulk.find(@q).remove
      assert_is_bulk_write_collection_view(result)
      assert_bulk_op_pushed [:delete, {:q => @q, :limit => 0}], @bulk
    end

    should "set :insert, :documents, terminate and return view for #insert" do
      document = {:a => 5}
      result = @bulk.insert(document)
      assert_is_bulk_write_collection_view(result)
      assert_bulk_op_pushed [:insert, {:d => document}], @bulk
    end

    should "handle spec examples" do
      @bulk = @collection.initialize_ordered_bulk_op

      # Update one document matching the selector
      @bulk.find({:a => 1}).update_one({"$inc" => { :x => 1 }})

      # Update all documents matching the selector
      @bulk.find({:a => 2}).update({"$inc" => { :x => 2 }})

      # Replace entire document (update with whole doc replace)
      @bulk.find({:a => 3}).replace_one({ :x => 3 })

      # Update one document matching the selector or upsert
      @bulk.find({:a => 1}).upsert.update_one({"$inc" => { :x => 1 }})

      # Update all documents matching the selector or upsert
      @bulk.find({:a => 2}).upsert.update({"$inc" => { :x => 2 }})

      # Replaces a single document matching the selector or upsert
      @bulk.find({:a => 3}).upsert.replace_one({ :x => 3 })

      # Remove a single document matching the selector
      @bulk.find({:a => 4}).remove_one()

      # Remove all documents matching the selector
      @bulk.find({:a => 5}).remove()

      # Insert a document
      @bulk.insert({ :x => 4 })

      # Execute the bulk operation, with an optional writeConcern overwriting the default w:1
      write_concern = {:w => 1, :j => 1}
      #@bulk.execute(write_concern)
    end

    should "execute, return result and reset @ops for #execute" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        @bulk.insert({ :x => 1 })
        @bulk.insert({ :x => 2 })
        write_concern = {:w => 1}
        result = @bulk.execute(write_concern)
        assert_equal([{"ok" => 2, "n" => 2}, {}, {"ok" => 1, "n" => 2}][wire_version], result, "wire_version:#{wire_version}")
        assert_equal 2, @collection.count
        assert_equal [], @bulk.ops
      end
    end

    should "run ordered big example" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        big_example(@bulk)
        write_concern = {:w => 1, :j => 1}
        result = @bulk.execute(write_concern)
        assert_equal_json(
            {
                "ok" => 4,
                "n" => 14,
                "upserted" => [
                    {
                        "index" => 10,
                        "_id" => BSON::ObjectId('52a1e4a4bb67fbc77e26a340')
                    }
                ]
            }, result, [{"ok" => 14}, {}, {}][wire_version], "wire_version:#{wire_version}")
        assert_false @collection.find.to_a.empty?
        assert_equal [{"x" => 3}, {"a" => 1, "x" => 2}, {"a" => 2, "x" => 4}, {"x" => 3}, {"x" => 4}], @collection.find.to_a.collect { |doc| doc.delete("_id"); doc }
      end
    end

    should "run unordered big example" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        @bulk = @collection.initialize_unordered_bulk_op
        big_example(@bulk)
        write_concern = {:w => 1, :j => 1}
        result = @bulk.execute(write_concern)
        # unordered varies, don't use assert_equal_json
        assert_false @collection.find.to_a.empty?
      end
    end

    should "run unordered bulk operations in one batch per write-type" do
      with_write_commands(@db.connection) do
        @collection.expects(:batch_write).at_most(3).returns([[],[],[],[]])
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
        result = bulk.execute
      end
    end

    should "handle error for duplicate key with offset" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        @bulk.find({:a => 1}).update_one({"$inc" => { :x => 1 }})
        @bulk.insert({:_id => 1, :a => 1})
        @bulk.insert({:_id => 1, :a => 2})
        @bulk.insert({:_id => 3, :a => 3})
        assert_bulk_exception(
            {
                "ok" => 1,
                "n" => 1,
                "code" => 65,
                "errmsg" => "batch item errors occurred",
                "errDetails" => [
                    {
                        "index" => 2,
                        "code" => 11000,
                        "errmsg" =>
                            "E11000 duplicate key error index: bulk_write_collection_view_test.test.$_id_  dup key: { : 1 }"
                    }
                ]
            }, [{'ok' => 2, 'n' => 1}, {}, {}][wire_version], "wire_version:#{wire_version}") { @bulk.execute }
      end
    end

    should "handle error for serialization with offset" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        assert_equal 16777216, @@client.max_bson_size
        @bulk.find({:a => 1}).update_one({"$inc" => { :x => 1 }})
        @bulk.insert({:_id => 1, :a => 1})
        @bulk.insert(generate_sized_doc(@@client.max_message_size + 1))
        @bulk.insert({:_id => 3, :a => 3})
        # errmsg varies, don't use assert_bulk_exception
        ex = assert_raise BulkWriteError do
          @bulk.execute
        end
        result = ex.result
        assert_equal(2, result['ok'], "wire_version:#{wire_version}")
        assert_equal(1, result['n'], "wire_version:#{wire_version}")
        err_details = result['errDetails']
        assert_equal(2, err_details.first['index'], "wire_version:#{wire_version}")
        assert_match(/too large/, err_details.first['errmsg'], "wire_version:#{wire_version}")
      end
    end

    should "handle errors for spec example 1 - handling errors" do # TODO - varies from spec
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        @collection.ensure_index(BSON::OrderedHash[:a, Mongo::ASCENDING], {:unique => true})
        bulk = @collection.initialize_ordered_bulk_op
        bulk.insert({:a => 1})
        bulk.find({:a => 1}).upsert.update({'$set' => {:a => 2}})
        bulk.insert({:a => 2})
        assert_bulk_exception(
            {
                "ok" => 2,
                "n" => 2,
                "code" => 65,
                "errmsg" => "batch item errors occurred",
                "errDetails" => [
                    {
                        "index" => 2,
                        "code" => 11000,
                        "errmsg" => "E11000 duplicate key error index: bulk_write_collection_view_test.test.$a_1  dup key: { : 2 }"
                    }
                ]
            }, [{'ok' => 2}, {}, {}][wire_version], "wire_version:#{wire_version}") { bulk.execute }
      end
    end

    should "handle errors for spec example 2 - with deferred write concern error" do
      # TODO - deferred write concern pending - requires replication
    end

    should "handle errors for spec example 3 - unordered tally of ok and n" do # TODO - varies from spec
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        @collection.ensure_index(BSON::OrderedHash[:a, Mongo::ASCENDING], {:unique => true})
        bulk = @collection.initialize_unordered_bulk_op
        bulk.insert({:a => 1})
        bulk.find({:a => 1}).upsert.update({'$set' => {:a => 2}})
        bulk.insert({:a => 2})
        # unordered varies, don't use assert_bulk_exception
        ex = assert_raise BulkWriteError do
          bulk.execute
        end
        result = ex.result
        assert_equal([2,nil,1][wire_version], result['ok'], "wire_version:#{wire_version}")
        assert_equal(2, result['n'], "wire_version:#{wire_version}")
        err_details = result['errDetails']
        assert_equal([2,nil,1][wire_version], err_details.first['index'], "wire_version:#{wire_version}")
        assert_match(/duplicate key error/, err_details.first['errmsg'], "wire_version:#{wire_version}")
       end
    end

    should "handle errors for spec example 4 - with deferred write concern error" do
      # TODO - deferred write concern pending - requires replication
    end

    should "handle errors for spec example 5 - rewrite index - missing update expression" do
      # TODO - can't reproduce missing update expression error
    end

    should "handle errors for spec example 6 - handling single upsert" do # chose array always for upserted value
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        @collection.ensure_index(BSON::OrderedHash[:a, Mongo::ASCENDING], {:unique => true})
        bulk = @collection.initialize_ordered_bulk_op
        bulk.find({:a => 1}).upsert.update({'$set' => {:a => 2}})
        result = bulk.execute
        assert_equal_json(
            {
                "ok" => 1,
                "n" => 1,
                "upserted" => [
                    {"_id" => BSON::ObjectId('52a16767bb67fbc77e26a310'), "index" => 0}
                ]
            }, result, {}, "wire_version:#{wire_version}")
      end
    end

    should "handle errors for spec example 7 - handling multiple upserts" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        @collection.ensure_index(BSON::OrderedHash[:a, Mongo::ASCENDING], {:unique => true})
        bulk = @collection.initialize_ordered_bulk_op
        bulk.find({:a => 1}).upsert.update({'$set' => {:a => 2}})
        bulk.find({:a => 3}).upsert.update({'$set' => {:a => 4}})
        result = bulk.execute
        assert_equal_json(
            {
                "ok" => 1,
                "n" => 2,
                "upserted" => [
                    {"index" => 0, "_id" => BSON::ObjectId('52a1e37cbb67fbc77e26a338')},
                    {"index" => 1, "_id" => BSON::ObjectId('52a1e37cbb67fbc77e26a339')}
                ]
            }, result, [{'ok' => 2}, {}, {}][wire_version], "wire_version:#{wire_version}")
      end
    end

    should "handle multiple errors for unordered bulk write" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @collection.remove
        @bulk = @collection.initialize_unordered_bulk_op
        @bulk.insert({:_id => 1, :a => 1})
        @bulk.insert({:_id => 1, :a => 2})
        @bulk.insert(generate_sized_doc(@@client.max_message_size + 1))
        @bulk.insert({:_id => 3, :a => 3})
        @bulk.find({:a => 4}).upsert.replace_one({ :x => 3 })
        # unordered varies, don't use assert_bulk_exception
        ex = assert_raise BulkWriteError do
          @bulk.execute
        end
        result = ex.result
        #assert_equal([3,nil,1][wire_version], result['ok'], "wire_version:#{wire_version}")
        assert_equal([3,nil,3][wire_version], result['n'], "wire_version:#{wire_version}")
        err_details = result['errDetails']
        # note: index is unavailable for the following with wire_version 0
        assert_match(/duplicate key error/, err_details.find{|e|e['code']==11000}['errmsg'], "wire_version:#{wire_version}")
        assert_match(/too large/, err_details.find{|e|e['index']==2}['errmsg'], "wire_version:#{wire_version}")
        assert_not_nil result['upserted'].find{|e|e['index']==4}
      end
    end

  end

end
