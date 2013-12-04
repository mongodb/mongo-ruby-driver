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
    public :batch_write_incremental
  end
  class BulkWriteCollectionView
    public :update_doc?, :replace_doc?, :sort_by_first_sym, :ordered_group_by_first

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

def assert_doc_equal_without_id(q, r)
  assert r, "result document should not be nil"
  assert_equal q.stringify_keys, r.except('_id')
end

def assert_bulk_op_pushed(expected, view)
  assert_equal expected, view.ops.last
end

def assert_is_bulk_write_collection_view(view)
  assert_equal Mongo::BulkWriteCollectionView, view.class
end

class BulkWriteCollectionViewTest < Test::Unit::TestCase
  @@client       ||= standard_connection(:op_timeout => 10)
  @@db           = @@client.db(MONGO_TEST_DB)
  @@test         = @@db.collection("test")
  @@version      = @@client.server_version

  DATABASE_NAME = 'bulk_write_collection_view_test'
  COLLECTION_NAME = 'test'

  def pp_with_caller(obj)
    puts "#{caller(1,1).first[/(.*):in/, 1]}:"
    pp obj
  end

  def default_setup
    @client = MongoClient.new
    @db = @client[DATABASE_NAME]
    @collection = @db[COLLECTION_NAME]
    @collection.remove
    @bulk = @collection.initialize_ordered_bulk_op
    @q = {:a => 1}
    @u = {"$inc" => { :x => 1 }}
    @r = {:b => 2}
  end

  def get_max_wire_version
    @db.connection.instance_variable_get(:@max_wire_version)
  end

  def set_max_wire_version(n)
    @db.connection.instance_variable_set(:@max_wire_version, n)
  end

  def with_max_wire_version(n)
    old_max_wire_version = get_max_wire_version
    new_max_wire_version = set_max_wire_version(n)
    yield
    set_max_wire_version(old_max_wire_version)
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

    should "check no top-leve key is operation for #replace_doc?" do
      assert_true @bulk.replace_doc?({ :x => 1 })
      assert_true @bulk.replace_doc?({})
      assert_false @bulk.replace_doc?({"$inc" => { :x => 1 }})
      assert_false @bulk.replace_doc?({ :a => 1, "$inc" => { :x => 1 }})
    end

    should "sort_by_first_sym for grouping unordered ops" do
      pairs = [
          [:insert, {:n => 0}],
          [:update, {:n => 1}], [:update, {:n => 2}],
          [:delete, {:n => 3}],
          [:insert, {:n => 5}], [:insert, {:n => 6}], [:insert, {:n => 7}],
          [:update, {:n => 8}],
          [:delete, {:n => 9}], [:delete, {:n => 10}]
      ]
      result = @bulk.sort_by_first_sym(pairs)
      expected = [
          :delete, :delete, :delete,
          :insert, :insert, :insert, :insert,
          :update, :update, :update
      ]
      assert_equal expected, result.collect{|first, rest| first}
    end

    should "calculate ordered_group_by_first" do
      pairs = [
          [:insert, {:n => 0}],
          [:update, {:n => 1}], [:update, {:n => 2}],
          [:delete, {:n => 3}],
          [:insert, {:n => 5}], [:insert, {:n => 6}], [:insert, {:n => 7}],
          [:update, {:n => 8}],
          [:delete, {:n => 9}], [:delete, {:n => 10}]
      ]
      result = @bulk.ordered_group_by_first(pairs)
      expected = [
          [:insert, [{:n => 0}]],
          [:update, [{:n => 1}, {:n => 2}]],
          [:delete, [{:n => 3}]],
          [:insert, [{:n => 5}, {:n => 6}, {:n => 7}]],
          [:update, [{:n => 8}]],
          [:delete, [{:n => 9}, {:n => 10}]]
      ]
      assert_equal expected, result
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
      assert_bulk_op_pushed [:insert, document], @bulk
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

      # Execute the bulk operation, with an optional writeConcern overwritting the default w:1
      write_concern = {:w => 1, :j => 1}
      #@bulk.execute(write_concern)
    end

    should "execute, return result and reset @ops for #execute" do
      @bulk.insert({ :x => 1 })
      @bulk.insert({ :x => 2 })
      write_concern = {:w => 1}
      result = @bulk.execute(write_concern)
      assert_equal 2, @collection.count
      assert_equal [], @bulk.ops
    end

    should "run ordered big example" do
      big_example(@bulk)
      write_concern = {:w => 1, :j => 1}
      result = @bulk.execute(write_concern)
      #pp_with_caller result
      assert_equal [{"x" => 3}, {"a" => 1, "x" => 2}, {"a" => 2, "x" => 4}, {"x" => 3}, {"x" => 4}], @collection.find.to_a.collect { |doc| doc.delete("_id"); doc }
    end

    should "run unordered big example" do
      @bulk = @collection.initialize_unordered_bulk_op
      big_example(@bulk)
      write_concern = {:w => 1, :j => 1}
      result = @bulk.execute(write_concern)
      #pp_with_caller result
      assert_false @collection.find.to_a.empty?
    end

    should "run old write operations with MIN_WIRE_VERSION" do
      with_max_wire_version(Mongo::MongoClient::MIN_WIRE_VERSION) do
        @bulk.insert({ :a => 1 })
        @bulk.insert({ :a => 2 })
        @bulk.insert({ :a => 3 })
        @bulk.insert({ :a => 4 })
        @bulk.insert({ :a => 5 })
        @bulk.find({:a => 1}).update_one({"$inc" => { :x => 1 }})
        @bulk.find({:a => 2}).update({"$inc" => { :x => 2 }})
        @bulk.find({:a => 4}).remove_one()
        @bulk.find({:a => 5}).remove()
        @bulk.insert({ :x => 3 })
        @bulk.find({:a => 3}).replace_one({ :x => 3 })
        @bulk.find({:x => 3}).remove()
        write_concern = {:w => 1, :j => 1}
        result = @bulk.execute(write_concern)
        assert_equal [{"a" => 1, "x" => 1}, {"a" => 2, "x" => 2}], @collection.find.to_a.collect { |doc| doc.delete("_id"); doc }
      end
    end

    should "run ordered bulk insert with serialization error" do
      @bulk.insert({:_id => 1, :a => 1})
      @bulk.insert({:_id => 1, :a => 2})
      @bulk.insert(generate_sized_doc(@@client.max_message_size + 1))
      @bulk.insert({:_id => 3, :a => 3})
      ex = assert_raise BulkWriteError do
        @bulk.execute
      end
      assert_equal Mongo::BulkWriteCollectionView::MULTIPLE_ERRORS_OCCURRED, ex.error_code
      assert_match(/too large/, ex.result[:errors].first.message)
      assert_equal [], @collection.find.to_a
    end

    should "run ordered bulk insert with duplicate key error" do
      @bulk.insert({:_id => 1, :a => 1})
      @bulk.insert({:_id => 1, :a => 2})
      @bulk.insert({:_id => 3, :a => 3})
      ex = assert_raise BulkWriteError do
        @bulk.execute
      end
      assert_equal Mongo::BulkWriteCollectionView::MULTIPLE_ERRORS_OCCURRED, ex.error_code
      assert_match(/duplicate key error/, ex.result[:errors].first.message)
      assert_equal [{"_id" => 1, "a" => 1}], @collection.find.to_a
    end

    should "run unordered bulk insert with errors" do
      @bulk = @collection.initialize_unordered_bulk_op
      @bulk.insert({:_id => 1, :a => 1})
      @bulk.insert({:_id => 1, :a => 2})
      @bulk.insert(generate_sized_doc(@@client.max_message_size + 1))
      @bulk.insert({:_id => 3, :a => 3})
      ex = assert_raise BulkWriteError do
        @bulk.execute
      end
      #pp_with_caller ex
      #pp_with_caller ex.result
      assert_equal Mongo::BulkWriteCollectionView::MULTIPLE_ERRORS_OCCURRED, ex.error_code
      assert_equal 2, ex.result[:errors].size
      assert_equal 1, ex.result[:exchanges].size
      assert_match(/too large/, ex.result[:errors].first.message)
      assert_match(/duplicate key error/, ex.result[:errors].last.message)
      #pp_with_caller ex.result[:errors].first.result
      assert_true ex.result[:errors].first.result.has_key?(:serialize)
      #pp_with_caller ex.result[:errors].last.result
      assert_equal [{"_id" => 1, "a" => 1}, {"_id" => 3, "a" => 3}], @collection.find.to_a
    end

    should "run unordered bulk operations in one batch per write-type" do
      @collection.expects(:batch_write_incremental).at_most(3).returns([[],[],[],[]])
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

end
