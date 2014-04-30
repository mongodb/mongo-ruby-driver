# Copyright (C) 2009-2013 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
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

require 'rbconfig'
require 'test_helper'

class CollectionTest < Test::Unit::TestCase
  @@client       ||= standard_connection(:op_timeout => 10)
  @@db           = @@client.db(TEST_DB)
  @@test         = @@db.collection("test")
  @@version      = @@client.server_version

  LIMITED_MAX_BSON_SIZE = 1024
  LIMITED_MAX_MESSAGE_SIZE = 3 * LIMITED_MAX_BSON_SIZE
  LIMITED_TEST_HEADROOM = 50
  LIMITED_VALID_VALUE_SIZE = LIMITED_MAX_BSON_SIZE - LIMITED_TEST_HEADROOM
  LIMITED_INVALID_VALUE_SIZE = LIMITED_MAX_BSON_SIZE + Mongo::MongoClient::COMMAND_HEADROOM + 1

  def setup
    @@test.remove
  end

  @@wv0 = Mongo::MongoClient::RELEASE_2_4_AND_BEFORE
  @@wv2 = Mongo::MongoClient::BATCH_COMMANDS
  @@a_h = Mongo::MongoClient::APPEND_HEADROOM
  @@s_h = Mongo::MongoClient::SERIALIZE_HEADROOM

  MAX_SIZE_EXCEPTION_TEST = [
    #[@@wv0, @@client.max_bson_size, nil, /xyzzy/], # succeeds standalone, fails whole suite
  ]
  MAX_SIZE_EXCEPTION_CRUBY_TEST = [
    [@@wv0, @@client.max_bson_size + 1,     BSON::InvalidDocument,   /Document.* too large/]
  ]
  MAX_SIZE_EXCEPTION_JRUBY_TEST = [
    [@@wv0, @@client.max_bson_size + 1,     Mongo::OperationFailure, /object to insert too large/]
  ]
  MAX_SIZE_EXCEPTION_COMMANDS_TEST = [
    #[@@wv2, @@client.max_bson_size,         nil,                     /xyzzy/], # succeeds standalone, fails whole suite
    [@@wv2, @@client.max_bson_size + 1,     Mongo::OperationFailure, /object to insert too large/],
    [@@wv2, @@client.max_bson_size + @@s_h, Mongo::OperationFailure, /object to insert too large/],
    [@@wv2, @@client.max_bson_size + @@a_h, BSON::InvalidDocument,   /Document.* too large/]
  ]

  @@max_size_exception_test = MAX_SIZE_EXCEPTION_TEST
  @@max_size_exception_test += MAX_SIZE_EXCEPTION_CRUBY_TEST unless RUBY_PLATFORM == 'java'
  #@@max_size_exception_test += MAX_SIZE_EXCEPTION_JRUBY_TEST if RUBY_PLATFORM == 'java'
  @@max_size_exception_test += MAX_SIZE_EXCEPTION_COMMANDS_TEST if @@version >= "2.5.2"

  def generate_sized_doc(size)
    doc = {"_id" => BSON::ObjectId.new, "x" => "y"}
    serialize_doc = BSON::BSON_CODER.serialize(doc, false, false, size)
    doc = {"_id" => BSON::ObjectId.new, "x" => "y" * (1 + size - serialize_doc.size)}
    assert_equal size, BSON::BSON_CODER.serialize(doc, false, false, size).size
    doc
  end

  def with_max_wire_version(client, wire_version) # does not support replica sets
    if client.wire_version_feature?(wire_version)
      client.class.class_eval(%Q{
        alias :old_max_wire_version :max_wire_version
        def max_wire_version
          #{wire_version}
        end
      })
      yield wire_version
      client.class.class_eval(%Q{
        alias :max_wire_version :old_max_wire_version
      })
    end
  end

  def test_insert_batch_max_sizes
    @@max_size_exception_test.each do |wire_version, size, exc, regexp|
      with_max_wire_version(@@client, wire_version) do
        @@test.remove
        doc = generate_sized_doc(size)
        begin
          @@test.insert([doc.dup])
          assert_equal nil, exc
        rescue => e
          assert_equal exc, e.class, "wire_version:#{wire_version}, size:#{size}, exc:#{exc} e:#{e.message.inspect} @@version:#{@@version}"
          assert_match regexp, e.message
        end
      end
    end
  end

  if @@version >= '2.5.4'

    def test_single_delete_write_command
      @@test.drop
      @@test.insert([{ :a => 1 }, { :a => 1 }])

      command = BSON::OrderedHash['delete', @@test.name,
                                  :deletes, [{ :q => { :a => 1 }, :limit => 1 }],
                                  :writeConcern, { :w => 1 },
                                  :ordered, false]

      result = @@db.command(command)
      assert_equal 1, result['n']
      assert_equal 1, result['ok']
      assert_equal 1, @@test.count
    end

    def test_multi_ordered_delete_write_command
      @@test.drop
      @@test.insert([{ :a => 1 }, { :a => 1 }])

      command = BSON::OrderedHash['delete', @@test.name,
                                  :deletes, [{ :q => { :a => 1 }, :limit => 0 }],
                                  :writeConcern, { :w => 1 },
                                  :ordered, true]

      result = @@db.command(command)
      assert_equal 2, result['n']
      assert_equal 1, result['ok']
      assert_equal 0, @@test.count
    end

    def test_multi_unordered_delete_write_command
      @@test.drop
      @@test.insert([{ :a => 1 }, { :a => 1 }])

      command = BSON::OrderedHash['delete', @@test.name,
                                  :deletes, [{ :q => { :a => 1 }, :limit => 0 }],
                                  :writeConcern, { :w => 1 },
                                  :ordered, false]

      result = @@db.command(command)
      assert_equal 2, result['n']
      assert_equal 1, result['ok']
      assert_equal 0, @@test.count
    end

    def test_delete_write_command_with_no_concern
      @@test.drop
      @@test.insert([{ :a => 1 }, { :a => 1 }])

      command = BSON::OrderedHash['delete', @@test.name,
                                  :deletes, [{ :q => { :a => 1 }, :limit => 0 }],
                                  :ordered, false]

      result = @@db.command(command)
      assert_equal 2, result['n']
      assert_equal 1, result['ok']
      assert_equal 0, @@test.count
    end

    def test_delete_write_command_with_error
      @@test.drop
      @@test.insert([{ :a => 1 }, { :a => 1 }])

      command = BSON::OrderedHash['delete', @@test.name,
                                  :deletes, [{ :q => { '$set' => { :a => 1 }}, :limit => 0 }],
                                  :writeConcern, { :w => 1 },
                                  :ordered, false]

      assert_raise Mongo::OperationFailure do
        @@db.command(command)
      end
    end

    def test_single_insert_write_command
      @@test.drop

      command = BSON::OrderedHash['insert', @@test.name,
                                  :documents, [{ :a => 1 }],
                                  :writeConcern, { :w => 1 },
                                  :ordered, false]

      result = @@db.command(command)
      assert_equal 1, result['ok']
      assert_equal 1, @@test.count
    end

    def test_multi_ordered_insert_write_command
      @@test.drop

      command = BSON::OrderedHash['insert', @@test.name,
                                  :documents, [{ :a => 1 }, { :a => 2 }],
                                  :writeConcern, { :w => 1 },
                                  :ordered, true]

      result = @@db.command(command)
      assert_equal 1, result['ok']
      assert_equal 2, @@test.count
    end

    def test_multi_unordered_insert_write_command
      @@test.drop

      command = BSON::OrderedHash['insert', @@test.name,
                                  :documents, [{ :a => 1 }, { :a => 2 }],
                                  :writeConcern, { :w => 1 },
                                  :ordered, false]

      result = @@db.command(command)
      assert_equal 1, result['ok']
      assert_equal 2, @@test.count
    end

    def test_insert_write_command_with_no_concern
      @@test.drop

      command = BSON::OrderedHash['insert', @@test.name,
                                  :documents, [{ :a => 1 }, { :a => 2 }],
                                  :ordered, false]

      result = @@db.command(command)
      assert_equal 1, result['ok']
      assert_equal 2, @@test.count
    end

    def test_insert_write_command_with_error
      @@test.drop
      @@test.ensure_index([[:a, 1]], { :unique => true })

      command = BSON::OrderedHash['insert', @@test.name,
                                  :documents, [{ :a => 1 }, { :a => 1 }],
                                  :writeConcern, { :w => 1 },
                                  :ordered, false]

      assert_raise Mongo::OperationFailure do
        @@db.command(command)
      end
    end

    def test_single_update_write_command
      @@test.drop
      @@test.insert([{ :a => 1 }, { :a => 2 }])

      command = BSON::OrderedHash['update', @@test.name,
                                  :updates, [{ :q => { :a => 1 }, :u => { '$set' => { :a => 2 }}}],
                                  :writeConcern, { :w => 1 }]

      result = @@db.command(command)
      assert_equal 1, result['ok']
      assert_equal 1, result['n']
      assert_equal 2, @@test.find({ :a => 2 }).count
    end

    def test_multi_ordered_update_write_command
      @@test.drop
      @@test.insert([{ :a => 1 }, { :a => 3 }])

      command = BSON::OrderedHash['update', @@test.name,
                                  :updates, [
                                              { :q => { :a => 1 }, :u => { '$set' => { :a => 2 }}},
                                              { :q => { :a => 3 }, :u => { '$set' => { :a => 4 }}}
                                            ],
                                  :writeConcern, { :w => 1 },
                                  :ordered, true]

      result = @@db.command(command)
      assert_equal 1, result['ok']
      assert_equal 2, result['n']
      assert_equal 1, @@test.find({ :a => 2 }).count
      assert_equal 1, @@test.find({ :a => 4 }).count
    end

    def test_multi_unordered_update_write_command
      @@test.drop
      @@test.insert([{ :a => 1 }, { :a => 3 }])

      command = BSON::OrderedHash['update', @@test.name,
                                  :updates, [
                                             { :q => { :a => 1 }, :u => { '$set' => { :a => 2 }}},
                                             { :q => { :a => 3 }, :u => { '$set' => { :a => 4 }}}
                                            ],
                                  :writeConcern, { :w => 1 },
                                  :ordered, false]

      result = @@db.command(command)
      assert_equal 1, result['ok']
      assert_equal 2, result['n']
      assert_equal 1, @@test.find({ :a => 2 }).count
      assert_equal 1, @@test.find({ :a => 4 }).count
    end

    def test_update_write_command_with_no_concern
      @@test.drop
      @@test.insert([{ :a => 1 }, { :a => 3 }])

      command = BSON::OrderedHash['update', @@test.name,
                                  :updates, [
                                             { :q => { :a => 1 }, :u => { '$set' => { :a => 2 }}},
                                             { :q => { :a => 3 }, :u => { '$set' => { :a => 4 }}}
                                            ],
                                  :ordered, false]

      result = @@db.command(command)
      assert_equal 1, result['ok']
      assert_equal 2, result['n']
      assert_equal 1, @@test.find({ :a => 2 }).count
      assert_equal 1, @@test.find({ :a => 4 }).count
    end

    def test_update_write_command_with_error
      @@test.drop
      @@test.ensure_index([[:a, 1]], { :unique => true })
      @@test.insert([{ :a => 1 }, { :a => 2 }])

      command = BSON::OrderedHash['update', @@test.name,
                                  :updates, [
                                             { :q => { :a => 2 }, :u => { '$set' => { :a => 1 }}}
                                            ],
                                  :ordered, false]

      assert_raise Mongo::OperationFailure do
        @@db.command(command)
      end
    end
  end

  if @@version >= '2.5.1'

    def test_aggregation_cursor
      [10, 1000].each do |size|
        @@test.drop
        size.times {|i| @@test.insert({ :_id => i }) }
        expected_sum = size.times.reduce(:+)

        cursor = @@test.aggregate(
          [{ :$project => {:_id => '$_id'}} ],
          :cursor => {}
        )

        assert_equal Mongo::Cursor, cursor.class

        cursor_sum = cursor.reduce(0) do |sum, doc|
          sum += doc['_id']
        end

        assert_equal expected_sum, cursor_sum
      end
      @@test.drop
    end

    def test_aggregation_array
      @@test.drop
      100.times {|i| @@test.insert({ :_id => i }) }
      agg = @@test.aggregate([{ :$project => {:_id => '$_id'}} ])

      assert agg.kind_of?(Array)

      @@test.drop
    end

    def test_aggregation_cursor_invalid_ops
      cursor = @@test.aggregate([], :cursor => {})
      assert_raise(Mongo::InvalidOperation) { cursor.rewind! }
      assert_raise(Mongo::InvalidOperation) { cursor.explain }
      assert_raise(Mongo::InvalidOperation) { cursor.count }
    end
  end

  def test_aggregation_invalid_read_pref
    assert_raise Mongo::MongoArgumentError do
      @@test.aggregate([], :read => :invalid_read_pref)
    end
  end

  if @@version >= '2.5.3'
    def test_aggregation_supports_explain
      @@db.expects(:command).with do |selector, opts|
        opts[:explain] == true
      end.returns({ 'ok' => 1 })
      @@test.aggregate([], :explain => true)
    end

    def test_aggregation_explain_returns_raw_result
      response = @@test.aggregate([], :explain => true)
      assert response['stages']
    end
  end

  def test_capped_method
    @@db.create_collection('normal')
    assert !@@db['normal'].capped?
    @@db.drop_collection('normal')

    @@db.create_collection('c', :capped => true, :size => 100_000)
    assert @@db['c'].capped?
    @@db.drop_collection('c')
  end

  def test_optional_pk_factory
    @coll_default_pk = @@db.collection('stuff')
    assert_equal BSON::ObjectId, @coll_default_pk.pk_factory
    @coll_default_pk = @@db.create_collection('more-stuff')
    assert_equal BSON::ObjectId, @coll_default_pk.pk_factory

    # Create a db with a pk_factory.
    @db = MongoClient.new(ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost',
                         ENV['MONGO_RUBY_DRIVER_PORT'] || MongoClient::DEFAULT_PORT).db(TEST_DB, :pk => Object.new)
    @coll = @db.collection('coll-with-pk')
    assert @coll.pk_factory.is_a?(Object)

    @coll = @db.create_collection('created_coll_with_pk')
    assert @coll.pk_factory.is_a?(Object)
  end

  class PKTest
    def self.create_pk
    end
  end

  def test_pk_factory_on_collection
    silently do
      @coll = Collection.new('foo', @@db, PKTest)
      assert_equal PKTest, @coll.pk_factory
    end

    @coll2 = Collection.new('foo', @@db, :pk => PKTest)
    assert_equal PKTest, @coll2.pk_factory
  end

  def test_valid_names
    assert_raise Mongo::InvalidNSName do
      @@db["te$t"]
    end

    assert_raise Mongo::InvalidNSName do
      @@db['$main']
    end

    assert @@db['$cmd']
    assert @@db['oplog.$main']
  end

  def test_collection
    assert_kind_of Collection, @@db["test"]
    assert_equal @@db["test"].name(), @@db.collection("test").name()
    assert_equal @@db["test"].name(), @@db[:test].name()

    assert_kind_of Collection, @@db["test"]["foo"]
    assert_equal @@db["test"]["foo"].name(), @@db.collection("test.foo").name()
    assert_equal @@db["test"]["foo"].name(), @@db["test.foo"].name()

    @@db["test"]["foo"].remove
    @@db["test"]["foo"].insert("x" => 5)
    assert_equal 5, @@db.collection("test.foo").find_one()["x"]
  end

  def test_rename_collection
    @@db.drop_collection('foo1')
    @@db.drop_collection('bar1')

    @col = @@db.create_collection('foo1')
    assert_equal 'foo1', @col.name

    @col.rename('bar1')
    assert_equal 'bar1', @col.name
  end

  def test_nil_id
    assert_equal 5, @@test.insert({"_id" => 5, "foo" => "bar"})
    assert_equal 5, @@test.save({"_id" => 5, "foo" => "baz"})
    assert_equal nil, @@test.find_one("foo" => "bar")
    assert_equal "baz", @@test.find_one(:_id => 5)["foo"]
    assert_raise OperationFailure do
      @@test.insert({"_id" => 5, "foo" => "bar"})
    end

    assert_equal nil, @@test.insert({"_id" => nil, "foo" => "bar"})
    assert_equal nil, @@test.save({"_id" => nil, "foo" => "baz"})
    assert_equal nil, @@test.find_one("foo" => "bar")
    assert_equal "baz", @@test.find_one(:_id => nil)["foo"]
    assert_raise OperationFailure do
      @@test.insert({"_id" => nil, "foo" => "bar"})
    end
    assert_raise OperationFailure do
      @@test.insert({:_id => nil, "foo" => "bar"})
    end
  end

  if @@version > "1.1"
    def setup_for_distinct
      @@test.remove
      @@test.insert([{:a => 0, :b => {:c => "a"}},
                     {:a => 1, :b => {:c => "b"}},
                     {:a => 1, :b => {:c => "c"}},
                     {:a => 2, :b => {:c => "a"}},
                     {:a => 3},
                     {:a => 3}])
    end

    def test_distinct_queries
      setup_for_distinct
      assert_equal [0, 1, 2, 3], @@test.distinct(:a).sort
      assert_equal ["a", "b", "c"], @@test.distinct("b.c").sort
    end

    if @@version >= "1.2"
      def test_filter_collection_with_query
        setup_for_distinct
        assert_equal [2, 3], @@test.distinct(:a, {:a => {"$gt" => 1}}).sort
      end

      def test_filter_nested_objects
        setup_for_distinct
        assert_equal ["a", "b"], @@test.distinct("b.c", {"b.c" => {"$ne" => "c"}}).sort
      end
    end
  end

  def test_safe_insert
    @@test.create_index("hello", :unique => true)
    begin
      a = {"hello" => "world"}
      @@test.insert(a)
      @@test.insert(a, :w => 0)
      assert(@@db.get_last_error['err'].include?("11000"))

      assert_raise OperationFailure do
        @@test.insert(a)
      end
    ensure
      @@test.drop_indexes
    end
  end

  def test_bulk_insert
    docs = []
    docs << {:foo => 1}
    docs << {:foo => 2}
    docs << {:foo => 3}
    response = @@test.insert(docs)
    assert_equal 3, response.length
    assert response.all? {|id| id.is_a?(BSON::ObjectId)}
    assert_equal 3, @@test.count
  end

  def test_bulk_insert_with_continue_on_error
    if @@version >= "2.0"
      @@test.create_index([["foo", 1]], :unique => true)
      begin
        docs = []
        docs << {:foo => 1}
        docs << {:foo => 1}
        docs << {:foo => 2}
        docs << {:foo => 3}
        assert_raise OperationFailure do
          @@test.insert(docs)
        end
        assert_equal 1, @@test.count
        @@test.remove

        docs = []
        docs << {:foo => 1}
        docs << {:foo => 1}
        docs << {:foo => 2}
        docs << {:foo => 3}
        assert_raise OperationFailure do
          @@test.insert(docs, :continue_on_error => true)
        end
        assert_equal 3, @@test.count

        @@test.remove
      ensure
        @@test.drop_index("foo_1")
      end
    end
  end

  def test_bson_valid_with_collect_on_error
    docs = []
    docs << {:foo => 1}
    docs << {:bar => 1}
    doc_ids, error_docs = @@test.insert(docs, :collect_on_error => true)
    assert_equal 2, @@test.count
    assert_equal 2, doc_ids.count
    assert_equal error_docs, []
  end

  def test_bson_invalid_key_serialize_error_with_collect_on_error
    docs = []
    docs << {:foo => 1}
    docs << {:bar => 1}
    invalid_docs = []
    invalid_docs << {'$invalid-key' => 1}
    invalid_docs << {'invalid.key'  => 1}
    docs += invalid_docs
    assert_raise BSON::InvalidKeyName do
      @@test.insert(docs, :collect_on_error => false)
    end
    assert_equal 2, @@test.count

    doc_ids, error_docs = @@test.insert(docs, :collect_on_error => true)
    assert_equal 2, @@test.count
    assert_equal 2, doc_ids.count
    assert_equal error_docs, invalid_docs
  end

  def test_bson_invalid_encoding_serialize_error_with_collect_on_error
    # Broken for current JRuby
    if RUBY_PLATFORM == 'java' then return end
    docs = []
    docs << {:foo => 1}
    docs << {:bar => 1}
    invalid_docs = []
    invalid_docs << {"\223\372\226}" => 1} # non utf8 encoding
    docs += invalid_docs

    assert_raise BSON::InvalidStringEncoding do
      @@test.insert(docs, :collect_on_error => false)
    end
    assert_equal 2, @@test.count

    doc_ids, error_docs = @@test.insert(docs, :collect_on_error => true)
    assert_equal 2, @@test.count
    assert_equal 2, doc_ids.count
    assert_equal error_docs, invalid_docs
  end

  def test_insert_one_error_doc_with_collect_on_error
    invalid_doc = {'$invalid-key' => 1}
    invalid_docs = [invalid_doc]
    doc_ids, error_docs = @@test.insert(invalid_docs, :collect_on_error => true)
    assert_equal [], doc_ids
    assert_equal [invalid_doc], error_docs
  end

  def test_insert_empty_docs_raises_exception
    assert_raise OperationFailure do
      @@test.insert([])
    end
  end

  def test_insert_empty_docs_with_collect_on_error_raises_exception
    assert_raise OperationFailure do
      @@test.insert([], :collect_on_error => true)
    end
  end

  def limited_collection
    conn = standard_connection(:connect => false)
    admin_db = Object.new
    admin_db.expects(:command).returns({
      'ok' => 1,
      'ismaster' => 1,
      'maxBsonObjectSize' => LIMITED_MAX_BSON_SIZE,
      'maxMessageSizeBytes' => LIMITED_MAX_MESSAGE_SIZE
    })
    conn.expects(:[]).with('admin').returns(admin_db)
    conn.connect
    return conn.db(TEST_DB)["test"]
  end

  def test_non_operation_failure_halts_insertion_with_continue_on_error
    coll = limited_collection
    coll.db.connection.stubs(:send_message_with_gle).raises(OperationTimeout).times(1)
    docs = []
    10.times do
      docs << {'foo' => 'a' * LIMITED_VALID_VALUE_SIZE}
    end
    assert_raise OperationTimeout do
      coll.insert(docs, :continue_on_error => true)
    end
  end

  def test_chunking_batch_insert
     docs = []
     10.times do
       docs << {'foo' => 'a' * LIMITED_VALID_VALUE_SIZE}
     end
     limited_collection.insert(docs)
     assert_equal 10, limited_collection.count
   end

  def test_chunking_batch_insert_without_collect_on_error
    docs = []
    4.times do
      docs << {'foo' => 'a' * LIMITED_VALID_VALUE_SIZE}
    end
    invalid_docs = []
    invalid_docs << {'$invalid-key' => 1} # non utf8 encoding
    docs += invalid_docs
    4.times do
      docs << {'foo' => 'a' * LIMITED_VALID_VALUE_SIZE}
    end
    assert_raise BSON::InvalidKeyName do
      limited_collection.insert(docs, :collect_on_error => false)
    end
  end

  def test_chunking_batch_insert_with_collect_on_error
   # Broken for current JRuby
   if RUBY_PLATFORM == 'java' then return end
    docs = []
    4.times do
      docs << {'foo' => 'a' * LIMITED_VALID_VALUE_SIZE}
    end
    invalid_docs = []
    invalid_docs << {'$invalid-key' => 1} # non utf8 encoding
    docs += invalid_docs
    4.times do
      docs << {'foo' => 'a' * LIMITED_VALID_VALUE_SIZE}
    end
    doc_ids, error_docs = limited_collection.insert(docs, :collect_on_error => true)
    assert_equal 8, doc_ids.count
    assert_equal doc_ids.count, limited_collection.count
    assert_equal error_docs, invalid_docs
  end

  def test_chunking_batch_insert_with_continue_on_error
    docs = []
    4.times do
      docs << {'foo' => 'a' * LIMITED_VALID_VALUE_SIZE}
    end
    docs << {'_id' => 'b', 'foo' => 'a'}
    docs << {'_id' => 'b', 'foo' => 'c'}
    4.times do
      docs << {'foo' => 'a' * LIMITED_VALID_VALUE_SIZE}
    end
    assert_raise OperationFailure do
      limited_collection.insert(docs, :continue_on_error => true)
    end
    assert limited_collection.count >= 6, "write commands need headroom for doc wrapping overhead - count:#{limited_collection.count}"
  end

  def test_chunking_batch_insert_without_continue_on_error
    docs = []
    4.times do
      docs << {'foo' => 'a' * LIMITED_VALID_VALUE_SIZE}
    end
    docs << {'_id' => 'b', 'foo' => 'a'}
    docs << {'_id' => 'b', 'foo' => 'c'}
    4.times do
      docs << {'foo' => 'a' * LIMITED_VALID_VALUE_SIZE}
    end
    assert_raise OperationFailure do
      limited_collection.insert(docs, :continue_on_error => false)
    end
    assert_equal 5, limited_collection.count
  end

  def test_maximum_insert_size
    docs = []
    3.times do
      docs << {'foo' => 'a' * LIMITED_VALID_VALUE_SIZE}
    end
    assert_equal limited_collection.insert(docs).length, 3
  end

  def test_maximum_document_size
    assert_raise InvalidDocument do
      limited_collection.insert({'foo' => 'a' * LIMITED_MAX_BSON_SIZE})
    end
  end

  def test_maximum_save_size
    assert limited_collection.save({'foo' => 'a' * LIMITED_VALID_VALUE_SIZE})
    assert_raise InvalidDocument do
      limited_collection.save({'foo' => 'a' * LIMITED_MAX_BSON_SIZE})
    end
  end

  def test_maximum_remove_size
    assert limited_collection.remove({'foo' => 'a' * LIMITED_VALID_VALUE_SIZE})
    assert_raise InvalidDocument do
      limited_collection.remove({'foo' => 'a' * LIMITED_MAX_BSON_SIZE})
    end
  end

  def test_maximum_update_size
    assert_raise InvalidDocument do
      limited_collection.update(
        {'foo' => 'a' * LIMITED_MAX_BSON_SIZE},
        {'foo' => 'a' * LIMITED_VALID_VALUE_SIZE}
      )
    end

    assert_raise InvalidDocument do
      limited_collection.update(
        {'foo' => 'a' * LIMITED_VALID_VALUE_SIZE},
        {'foo' => 'a' * LIMITED_MAX_BSON_SIZE}
      )
    end

    assert_raise InvalidDocument do
      limited_collection.update(
        {'foo' => 'a' * LIMITED_MAX_BSON_SIZE},
        {'foo' => 'a' * LIMITED_MAX_BSON_SIZE}
      )
    end

    assert limited_collection.update(
      {'foo' => 'a' * LIMITED_VALID_VALUE_SIZE},
      {'foo' => 'a' * LIMITED_VALID_VALUE_SIZE}
    )
  end

  def test_maximum_query_size
    assert limited_collection.find({'foo' => 'a' * LIMITED_VALID_VALUE_SIZE}).to_a
    assert limited_collection.find(
      {'foo' => 'a' * LIMITED_VALID_VALUE_SIZE},
      {:fields => {'foo' => 'a' * LIMITED_VALID_VALUE_SIZE}}
    ).to_a

    assert_raise InvalidDocument do
      limited_collection.find({'foo' => 'a' * LIMITED_INVALID_VALUE_SIZE}).to_a
    end

    assert_raise InvalidDocument do
      limited_collection.find(
        {'foo' => 'a' * LIMITED_VALID_VALUE_SIZE},
        {:fields => {'foo' => 'a' * LIMITED_MAX_BSON_SIZE}}
      ).to_a
    end
  end

  #if @@version >= "1.5.1"
  #  def test_safe_mode_with_advanced_safe_with_invalid_options
  #    assert_raise_error ArgumentError, "Unknown key(s): wtime" do
  #      @@test.insert({:foo => 1}, :w => 2, :wtime => 1, :fsync => true)
  #    end
  #    assert_raise_error ArgumentError, "Unknown key(s): wtime" do
  #      @@test.update({:foo => 1}, {:foo => 2}, :w => 2, :wtime => 1, :fsync => true)
  #    end
  #
  #    assert_raise_error ArgumentError, "Unknown key(s): wtime" do
  #      @@test.remove({:foo => 2}, :w => 2, :wtime => 1, :fsync => true)
  #    end
  #  end
  #end

  def test_safe_mode_with_journal_commit_option
    with_default_journaling(@@client) do
      @@test.insert({:foo => 1}, :j => true)
      @@test.update({:foo => 1}, {:foo => 2}, :j => true)
      @@test.remove({:foo => 2}, :j => true)
    end
  end

  if @@version < "2.5.3"
    def test_jnote_raises_exception
      with_no_journaling(@@client) do
        ex = assert_raise Mongo::WriteConcernError do
          @@test.insert({:foo => 1}, :j => true)
        end
        result = ex.result
        assert_true result.has_key?("jnote")
      end
    end

    def test_wnote_raises_exception_with_err_not_nil
      ex = assert_raise Mongo::WriteConcernError do
        @@test.insert({:foo => 1}, :w => 2)
      end
      result = ex.result
      assert_not_nil result["err"]
      assert_true result.has_key?("wnote")
    end
  end

  def test_update
    id1 = @@test.save("x" => 5)
    @@test.update({}, {"$inc" => {"x" => 1}})
    assert_equal 1, @@test.count()
    assert_equal 6, @@test.find_one(:_id => id1)["x"]

    id2 = @@test.save("x" => 1)
    @@test.update({"x" => 6}, {"$inc" => {"x" => 1}})
    assert_equal 7, @@test.find_one(:_id => id1)["x"]
    assert_equal 1, @@test.find_one(:_id => id2)["x"]
  end

  if @@version < "2.5.3"
    def test_update_check_keys
      @@test.save("x" => 1)
      @@test.update({"x" => 1}, {"$set" => {"a.b" => 2}})
      assert_equal 2, @@test.find_one("x" => 1)["a"]["b"]

      assert_raise_error BSON::InvalidKeyName do
        @@test.update({"x" => 1}, {"a.b" => 3})
      end
    end
  end

  if @@version >= "1.1.3"
    def test_multi_update
      @@test.save("num" => 10)
      @@test.save("num" => 10)
      @@test.save("num" => 10)
      assert_equal 3, @@test.count

      @@test.update({"num" => 10}, {"$set" => {"num" => 100}}, :multi => true)
      @@test.find.each do |doc|
        assert_equal 100, doc["num"]
      end
    end
  end

  def test_upsert
    @@test.update({"page" => "/"}, {"$inc" => {"count" => 1}}, :upsert => true)
    @@test.update({"page" => "/"}, {"$inc" => {"count" => 1}}, :upsert => true)

    assert_equal 1, @@test.count()
    assert_equal 2, @@test.find_one()["count"]
  end

  if @@version < "1.1.3"
    def test_safe_update
      @@test.create_index("x")
      @@test.insert("x" => 5)

      @@test.update({}, {"$inc" => {"x" => 1}})
      assert @@db.error?

      # Can't change an index.
      assert_raise OperationFailure do
        @@test.update({}, {"$inc" => {"x" => 1}})
      end
      @@test.drop
    end
  else
    def test_safe_update
      @@test.create_index("x", :unique => true)
      @@test.insert("x" => 5)
      @@test.insert("x" => 10)

      # Can update an indexed collection.
      @@test.update({}, {"$inc" => {"x" => 1}})
      assert !@@db.error?

      # Can't duplicate an index.
      assert_raise OperationFailure do
        @@test.update({}, {"x" => 10})
      end
      @@test.drop
    end
  end

  def test_safe_save
    @@test.create_index("hello", :unique => true)

    @@test.save("hello" => "world")
    @@test.save({"hello" => "world"}, :w => 0)

    assert_raise OperationFailure do
      @@test.save({"hello" => "world"})
    end
    @@test.drop
  end

  def test_mocked_safe_remove
    @client = standard_connection
    @db   = @client[TEST_DB]
    @test = @db['test-safe-remove']
    @test.save({:a => 20})
    @client.stubs(:receive).returns([[{'ok' => 0, 'err' => 'failed'}], 1, 0])

    assert_raise OperationFailure do
      @test.remove({})
    end
    @test.drop
  end

  def test_safe_remove
    @client = standard_connection
    @db   = @client[TEST_DB]
    @test = @db['test-safe-remove']
    @test.remove
    @test.save({:a => 50})
    assert_equal 1, @test.remove({})["n"]
    @test.drop
  end

  def test_remove_return_value
    assert_equal true, @@test.remove({}, :w => 0)
  end

  def test_remove_with_limit
    @@test.insert([{:n => 1},{:n => 2},{:n => 3}])
    @@test.remove({}, :limit => 1)
    assert_equal 2, @@test.count
    @@test.remove({}, :limit => 0)
    assert_equal 0, @@test.count
  end

  def test_count
    @@test.drop

    assert_equal 0, @@test.count
    @@test.save(:x => 1)
    @@test.save(:x => 2)
    assert_equal 2, @@test.count

    assert_equal 1, @@test.count(:query => {:x => 1})
    assert_equal 1, @@test.count(:limit => 1)
    assert_equal 0, @@test.count(:skip => 2)
  end

  # Note: #size is just an alias for #count.
  def test_size
    @@test.drop

    assert_equal 0, @@test.count
    assert_equal @@test.size, @@test.count
    @@test.save("x" => 1)
    @@test.save("x" => 2)
    assert_equal @@test.size, @@test.count
  end

  def test_no_timeout_option
    @@test.drop

    assert_raise ArgumentError, "Timeout can be set to false only when #find is invoked with a block." do
      @@test.find({}, :timeout => false)
    end

    @@test.find({}, :timeout => false) do |cursor|
      assert_equal 0, cursor.count
    end

    @@test.save("x" => 1)
    @@test.save("x" => 2)
    @@test.find({}, :timeout => false) do |cursor|
      assert_equal 2, cursor.count
    end
  end

  def test_default_timeout
    cursor = @@test.find
    assert_equal true, cursor.timeout
  end

  def test_fields_as_hash
    @@test.save(:a => 1, :b => 1, :c => 1)

    doc = @@test.find_one({:a => 1}, :fields => {:b => 0})
    assert_nil doc['b']
    assert doc['a']
    assert doc['c']

    doc = @@test.find_one({:a => 1}, :fields => {:a => 1, :b => 1})
    assert_nil doc['c']
    assert doc['a']
    assert doc['b']


    assert_raise Mongo::OperationFailure do
      @@test.find_one({:a => 1}, :fields => {:a => 1, :b => 0})
    end
  end

  if @@version >= '2.5.5'
    def test_meta_field_projection
      @@test.save({ :t => 'spam eggs and spam'})
      @@test.save({ :t => 'spam'})
      @@test.save({ :t => 'egg sausage and bacon'})

      @@test.ensure_index([[:t, 'text']])
      assert @@test.find_one({ :$text => { :$search => 'spam' }},
                             { :fields => [:t, { :score => { :$meta => 'textScore' } }] })
    end

    def test_sort_by_meta
      @@test.save({ :t => 'spam eggs and spam'})
      @@test.save({ :t => 'spam'})
      @@test.save({ :t => 'egg sausage and bacon'})

      @@test.ensure_index([[:t, 'text']])
      assert @@test.find({ :$text => { :$search => 'spam' }}).sort([:score, { '$meta' => 'textScore' }])
      assert @@test.find({ :$text => { :$search => 'spam' }}).sort(:score => { '$meta' =>'textScore' })
    end
  end

  if @@version >= "1.5.1"
    def test_fields_with_slice
      @@test.save({:foo => [1, 2, 3, 4, 5, 6], :test => 'slice'})

      doc = @@test.find_one({:test => 'slice'}, :fields => {'foo' => {'$slice' => [0, 3]}})
      assert_equal [1, 2, 3], doc['foo']
      @@test.remove
    end
  end

  def test_find_one
    id = @@test.save("hello" => "world", "foo" => "bar")

    assert_equal "world", @@test.find_one()["hello"]
    assert_equal @@test.find_one(id), @@test.find_one()
    assert_equal @@test.find_one(nil), @@test.find_one()
    assert_equal @@test.find_one({}), @@test.find_one()
    assert_equal @@test.find_one("hello" => "world"), @@test.find_one()
    assert_equal @@test.find_one(BSON::OrderedHash["hello", "world"]), @@test.find_one()

    assert @@test.find_one(nil, :fields => ["hello"]).include?("hello")
    assert !@@test.find_one(nil, :fields => ["foo"]).include?("hello")
    assert_equal ["_id"], @@test.find_one(nil, :fields => []).keys()

    assert_equal nil, @@test.find_one("hello" => "foo")
    assert_equal nil, @@test.find_one(BSON::OrderedHash["hello", "foo"])
    assert_equal nil, @@test.find_one(ObjectId.new)

    assert_raise TypeError do
      @@test.find_one(6)
    end
  end

  def test_find_one_with_max_time_ms
    with_forced_timeout(@@client) do
      assert_raise ExecutionTimeout do
        @@test.find_one({}, { :max_time_ms => 100 })
      end
    end
  end

  def test_find_one_with_compile_regex_option
    regex = /.*/
    @@test.insert('r' => /.*/)
    assert_kind_of Regexp, @@test.find_one({})['r']
    assert_kind_of Regexp, @@test.find_one({}, :compile_regex => true)['r']
    assert_equal BSON::Regex, @@test.find_one({}, :compile_regex => false)['r'].class
  end

  def test_insert_adds_id
    doc = {"hello" => "world"}
    @@test.insert(doc)
    assert(doc.include?(:_id))

    docs = [{"hello" => "world"}, {"hello" => "world"}]
    @@test.insert(docs)
    docs.each do |d|
      assert(d.include?(:_id))
    end
  end

  def test_save_adds_id
    doc = {"hello" => "world"}
    @@test.save(doc)
    assert(doc.include?(:_id))
  end

  def test_optional_find_block
    10.times do |i|
      @@test.save("i" => i)
    end

    x = nil
    @@test.find("i" => 2) { |cursor|
      x = cursor.count()
    }
    assert_equal 1, x

    i = 0
    @@test.find({}, :skip => 5) do |cursor|
      cursor.each do |doc|
        i = i + 1
      end
    end
    assert_equal 5, i

    c = nil
    @@test.find() do |cursor|
      c = cursor
    end
    assert c.closed?
  end

  def setup_aggregate_data
    # save some data
    @@test.save( {
        "_id" => 1,
        "title" => "this is my title",
        "author" => "bob",
        "posted" => Time.utc(2000),
        "pageViews" => 5 ,
        "tags" => [ "fun" , "good" , "fun" ],
        "comments" => [
                        { "author" => "joe", "text" => "this is cool" },
                        { "author" => "sam", "text" => "this is bad" }
            ],
        "other" => { "foo" => 5 }
        } )

    @@test.save( {
         "_id" => 2,
         "title" => "this is your title",
         "author" => "dave",
         "posted" => Time.utc(2001),
         "pageViews" => 7,
         "tags" => [ "fun" , "nasty" ],
         "comments" => [
                         { "author" => "barbara" , "text" => "this is interesting" },
                         { "author" => "jenny", "text" => "i like to play pinball", "votes" => 10 }
         ],
          "other" => { "bar" => 14 }
        })

    @@test.save( {
            "_id" => 3,
            "title" => "this is some other title",
            "author" => "jane",
            "posted" => Time.utc(2002),
            "pageViews" => 6 ,
            "tags" => [ "nasty", "filthy" ],
            "comments" => [
                { "author" => "will" , "text" => "i don't like the color" } ,
                { "author" => "jenny" , "text" => "can i get that in green?" }
            ],
            "other" => { "bar" => 14 }
        })

  end

  if @@version > '2.1.1'
    def test_reponds_to_aggregate
      assert_respond_to @@test, :aggregate
    end

    def test_aggregate_requires_arguments
      assert_raise MongoArgumentError do
        @@test.aggregate()
      end
    end

    def test_aggregate_requires_valid_arguments
      assert_raise MongoArgumentError do
        @@test.aggregate({})
      end
    end

    def test_aggregate_pipeline_operator_format
      assert_raise Mongo::OperationFailure do
        @@test.aggregate([{"$project" => "_id"}])
      end
    end

    def test_aggregate_pipeline_operators_using_strings
      setup_aggregate_data
      desired_results = [ {"_id"=>1, "pageViews"=>5, "tags"=>["fun", "good", "fun"]},
                          {"_id"=>2, "pageViews"=>7, "tags"=>["fun", "nasty"]},
                          {"_id"=>3, "pageViews"=>6, "tags"=>["nasty", "filthy"]} ]
      results = @@test.aggregate([{"$project" => {"tags" => 1, "pageViews" => 1}}])
      assert_equal desired_results, results
    end

    def test_aggregate_pipeline_operators_using_symbols
      setup_aggregate_data
      desired_results = [ {"_id"=>1, "pageViews"=>5, "tags"=>["fun", "good", "fun"]},
                          {"_id"=>2, "pageViews"=>7, "tags"=>["fun", "nasty"]},
                          {"_id"=>3, "pageViews"=>6, "tags"=>["nasty", "filthy"]} ]
      results = @@test.aggregate([{"$project" => {:tags => 1, :pageViews => 1}}])
      assert_equal desired_results, results
    end

    def test_aggregate_pipeline_multiple_operators
      setup_aggregate_data
      results = @@test.aggregate([{"$project" => {"tags" => 1, "pageViews" => 1}}, {"$match" => {"pageViews" => 7}}])
      assert_equal 1, results.length
    end

    def test_aggregate_pipeline_unwind
      setup_aggregate_data
      desired_results = [ {"_id"=>1, "title"=>"this is my title", "author"=>"bob", "posted"=>Time.utc(2000),
                          "pageViews"=>5, "tags"=>"fun", "comments"=>[{"author"=>"joe", "text"=>"this is cool"},
                            {"author"=>"sam", "text"=>"this is bad"}], "other"=>{"foo"=>5 } },
                          {"_id"=>1, "title"=>"this is my title", "author"=>"bob", "posted"=>Time.utc(2000),
                            "pageViews"=>5, "tags"=>"good", "comments"=>[{"author"=>"joe", "text"=>"this is cool"},
                            {"author"=>"sam", "text"=>"this is bad"}], "other"=>{"foo"=>5 } },
                          {"_id"=>1, "title"=>"this is my title", "author"=>"bob", "posted"=>Time.utc(2000),
                            "pageViews"=>5, "tags"=>"fun", "comments"=>[{"author"=>"joe", "text"=>"this is cool"},
                              {"author"=>"sam", "text"=>"this is bad"}], "other"=>{"foo"=>5 } },
                          {"_id"=>2, "title"=>"this is your title", "author"=>"dave", "posted"=>Time.utc(2001),
                            "pageViews"=>7, "tags"=>"fun", "comments"=>[{"author"=>"barbara", "text"=>"this is interesting"},
                              {"author"=>"jenny", "text"=>"i like to play pinball", "votes"=>10 }], "other"=>{"bar"=>14 } },
                          {"_id"=>2, "title"=>"this is your title", "author"=>"dave", "posted"=>Time.utc(2001),
                            "pageViews"=>7, "tags"=>"nasty", "comments"=>[{"author"=>"barbara", "text"=>"this is interesting"},
                              {"author"=>"jenny", "text"=>"i like to play pinball", "votes"=>10 }], "other"=>{"bar"=>14 } },
                          {"_id"=>3, "title"=>"this is some other title", "author"=>"jane", "posted"=>Time.utc(2002),
                            "pageViews"=>6, "tags"=>"nasty", "comments"=>[{"author"=>"will", "text"=>"i don't like the color"},
                              {"author"=>"jenny", "text"=>"can i get that in green?"}], "other"=>{"bar"=>14 } },
                          {"_id"=>3, "title"=>"this is some other title", "author"=>"jane", "posted"=>Time.utc(2002),
                            "pageViews"=>6, "tags"=>"filthy", "comments"=>[{"author"=>"will", "text"=>"i don't like the color"},
                              {"author"=>"jenny", "text"=>"can i get that in green?"}], "other"=>{"bar"=>14 } }
                          ]
      results = @@test.aggregate([{"$unwind"=> "$tags"}])
      assert_equal desired_results, results
    end

    def test_aggregate_with_compile_regex_option
      # see SERVER-6470
      return unless @@version >= '2.3.2'
      @@test.insert({ 'r' => /.*/ })
      result1 = @@test.aggregate([])
      assert_kind_of Regexp, result1.first['r']

      result2 = @@test.aggregate([], :compile_regex => false)
      assert_kind_of BSON::Regex, result2.first['r']

      return unless @@version >= '2.5.1'
      result = @@test.aggregate([], :compile_regex => false, :cursor => {})
      assert_kind_of BSON::Regex, result.first['r']
    end
  end

  if @@version >= "2.5.2"
    def test_out_aggregate
      out_collection = 'test_out'
      @@db.drop_collection(out_collection)
      setup_aggregate_data
      docs = @@test.find.to_a
      pipeline = [{:$out => out_collection}]
      @@test.aggregate(pipeline)
      assert_equal docs, @@db.collection(out_collection).find.to_a
    end

    def test_out_aggregate_nonprimary_sym_warns
      ReadPreference::expects(:warn).with(regexp_matches(/rerouted to primary/))
      pipeline = [{:$out => 'test_out'}]
      @@test.aggregate(pipeline, :read => :secondary)
    end

    def test_out_aggregate_nonprimary_string_warns
      ReadPreference::expects(:warn).with(regexp_matches(/rerouted to primary/))
      pipeline = [{'$out' => 'test_out'}]
      @@test.aggregate(pipeline, :read => :secondary)
    end

    def test_out_aggregate_string_returns_raw_response
      pipeline = [{'$out' => 'test_out'}]
      response = @@test.aggregate(pipeline)
      assert response.respond_to?(:keys)
    end

    def test_out_aggregate_sym_returns_raw_response
      pipeline = [{:$out => 'test_out'}]
      response = @@test.aggregate(pipeline)
      assert response.respond_to?(:keys)
    end
  end

  if @@version > "1.1.1"
    def test_map_reduce
      @@test << { "user_id" => 1 }
      @@test << { "user_id" => 2 }

      m = "function() { emit(this.user_id, 1); }"
      r = "function(k,vals) { return 1; }"
      res = @@test.map_reduce(m, r, :out => 'foo')
      assert res.find_one({"_id" => 1})
      assert res.find_one({"_id" => 2})
    end

    def test_map_reduce_with_code_objects
      @@test << { "user_id" => 1 }
      @@test << { "user_id" => 2 }

      m = Code.new("function() { emit(this.user_id, 1); }")
      r = Code.new("function(k,vals) { return 1; }")
      res = @@test.map_reduce(m, r, :out => 'foo')
      assert res.find_one({"_id" => 1})
      assert res.find_one({"_id" => 2})
    end

    def test_map_reduce_with_options
      @@test.remove
      @@test << { "user_id" => 1 }
      @@test << { "user_id" => 2 }
      @@test << { "user_id" => 3 }

      m = Code.new("function() { emit(this.user_id, 1); }")
      r = Code.new("function(k,vals) { return 1; }")
      res = @@test.map_reduce(m, r, :query => {"user_id" => {"$gt" => 1}}, :out => 'foo')
      assert_equal 2, res.count
      assert res.find_one({"_id" => 2})
      assert res.find_one({"_id" => 3})
    end

    def test_map_reduce_with_raw_response
      m = Code.new("function() { emit(this.user_id, 1); }")
      r = Code.new("function(k,vals) { return 1; }")
      res = @@test.map_reduce(m, r, :raw => true, :out => 'foo')
      assert res["result"]
      assert res["counts"]
      assert res["timeMillis"]
    end

    def test_map_reduce_with_output_collection
      output_collection = "test-map-coll"
      m = Code.new("function() { emit(this.user_id, 1); }")
      r = Code.new("function(k,vals) { return 1; }")
      res = @@test.map_reduce(m, r, :raw => true, :out => output_collection)
      assert_equal output_collection, res["result"]
      assert res["counts"]
      assert res["timeMillis"]
    end

    def test_map_reduce_nonprimary_output_collection_reroutes
      output_collection = "test-map-coll"
      m = Code.new("function() { emit(this.user_id, 1); }")
      r = Code.new("function(k,vals) { return 1; }")
      Mongo::ReadPreference.expects(:warn).with(regexp_matches(/rerouted to primary/))
      res = @@test.map_reduce(m, r, :raw => true, :out => output_collection, :read => :secondary)
    end

    if @@version >= "1.8.0"
      def test_map_reduce_with_collection_merge
        @@test << {:user_id => 1}
        @@test << {:user_id => 2}
        output_collection = "test-map-coll"
        m = Code.new("function() { emit(this.user_id, {count: 1}); }")
        r = Code.new("function(k,vals) { var sum = 0;" +
          " vals.forEach(function(v) { sum += v.count;} ); return {count: sum}; }")
        res = @@test.map_reduce(m, r, :out => output_collection)

        @@test.remove
        @@test << {:user_id => 3}
        res = @@test.map_reduce(m, r, :out => {:merge => output_collection})
        assert res.find.to_a.any? {|doc| doc["_id"] == 3 && doc["value"]["count"] == 1}

        @@test.remove
        @@test << {:user_id => 3}
        res = @@test.map_reduce(m, r, :out => {:reduce => output_collection})
        assert res.find.to_a.any? {|doc| doc["_id"] == 3 && doc["value"]["count"] == 2}

        assert_raise ArgumentError do
          @@test.map_reduce(m, r, :out => {:inline => 1})
        end

        @@test.map_reduce(m, r, :raw => true, :out => {:inline => 1})
        assert res["results"]
      end

      def test_map_reduce_with_collection_output_to_other_db
        @@test << {:user_id => 1}
        @@test << {:user_id => 2}

        m = Code.new("function() { emit(this.user_id, 1); }")
        r = Code.new("function(k,vals) { return 1; }")
        oh = BSON::OrderedHash.new
        oh[:replace] = 'foo'
        oh[:db] = TEST_DB
        res = @@test.map_reduce(m, r, :out => (oh))
        assert res["result"]
        assert res["counts"]
        assert res["timeMillis"]
        assert res.find.to_a.any? {|doc| doc["_id"] == 2 && doc["value"] == 1}
      end
    end
  end

  if @@version >= '2.5.5'
    def test_aggregation_allow_disk_use
      @@db.expects(:command).with do |selector, opts|
        opts[:allowDiskUse] == true
      end.returns({ 'ok' => 1 })
      @@test.aggregate([], :allowDiskUse => true)
    end

    def test_parallel_scan
      8000.times { |i| @@test.insert({ :_id => i }) }

      lock = Mutex.new
      doc_ids = Set.new
      threads = []
      cursors = @@test.parallel_scan(3)
      cursors.each_with_index do |cursor, i|
        threads << Thread.new do
          docs = cursor.to_a
          lock.synchronize do
            docs.each do |doc|
              doc_ids << doc['_id']
            end
          end
        end
      end
      threads.each(&:join)
      assert_equal 8000, doc_ids.count
    end
  end

  if @@version > "1.3.0"
    def test_find_and_modify
      @@test << { :a => 1, :processed => false }
      @@test << { :a => 2, :processed => false }
      @@test << { :a => 3, :processed => false }

      @@test.find_and_modify(:query => {},
                             :sort => [['a', -1]],
                             :update => {"$set" => {:processed => true}})

      assert @@test.find_one({:a => 3})['processed']
    end

    def test_find_and_modify_with_invalid_options
      @@test << { :a => 1, :processed => false }
      @@test << { :a => 2, :processed => false }
      @@test << { :a => 3, :processed => false }

      assert_raise Mongo::OperationFailure do
        @@test.find_and_modify(:blimey => {})
      end
    end

    def test_find_and_modify_with_full_response
      @@test << { :a => 1, :processed => false }
      @@test << { :a => 2, :processed => false }
      @@test << { :a => 3, :processed => false }

      doc = @@test.find_and_modify(:query => {},
                                   :sort => [['a', -1]],
                                   :update => {"$set" => {:processed => true}},
                                   :full_response => true,
                                   :new => true)

      assert doc['value']['processed']
      assert ['ok', 'value', 'lastErrorObject'].all? { |key| doc.key?(key) }
    end
  end

  if @@version >= "1.3.5"
    def test_coll_stats
      @@test << {:n => 1}
      @@test.create_index("n")

      assert_equal "#{TEST_DB}.test", @@test.stats['ns']
      @@test.drop
    end
  end

  def test_saving_dates_pre_epoch
    if RbConfig::CONFIG['host_os'] =~ /mswin|mingw|cygwin/ then return true end
    begin
      @@test.save({'date' => Time.utc(1600)})
      assert_in_delta Time.utc(1600), @@test.find_one()["date"], 2
    rescue ArgumentError
      # See note in test_date_before_epoch (BSONTest)
    end
  end

  def test_save_symbol_find_string
    @@test.save(:foo => :mike)

    assert_equal :mike, @@test.find_one(:foo => :mike)["foo"]
    assert_equal :mike, @@test.find_one("foo" => :mike)["foo"]

    # TODO enable these tests conditionally based on server version (if >1.0)
    # assert_equal :mike, @@test.find_one(:foo => "mike")["foo"]
    # assert_equal :mike, @@test.find_one("foo" => "mike")["foo"]
  end

  def test_batch_size
    n_docs = 6
    batch_size = n_docs/2
    n_docs.times do |i|
      @@test.save(:foo => i)
    end

    doc_count = 0
    cursor = @@test.find({}, :batch_size => batch_size)
    cursor.next
    assert_equal batch_size, cursor.instance_variable_get(:@returned)
    doc_count += batch_size
    batch_size.times { cursor.next }
    assert_equal doc_count + batch_size, cursor.instance_variable_get(:@returned)
    doc_count += batch_size
    assert_equal n_docs, doc_count
  end

  def test_batch_size_with_smaller_limit
    n_docs = 6
    batch_size = n_docs/2
    n_docs.times do |i|
      @@test.insert(:foo => i)
    end

    cursor = @@test.find({}, :batch_size => batch_size, :limit => 2)
    cursor.next
    assert_equal 2, cursor.instance_variable_get(:@returned)
  end

  def test_batch_size_with_larger_limit
    n_docs = 6
    batch_size = n_docs/2
    n_docs.times do |i|
      @@test.insert(:foo => i)
    end

    doc_count = 0
    cursor = @@test.find({}, :batch_size => batch_size, :limit => n_docs + 5)
    cursor.next
    assert_equal batch_size, cursor.instance_variable_get(:@returned)
    doc_count += batch_size
    batch_size.times { cursor.next }
    assert_equal doc_count + batch_size, cursor.instance_variable_get(:@returned)
    doc_count += batch_size
    assert_equal n_docs, doc_count
end

  def test_batch_size_with_negative_limit
    n_docs = 6
    batch_size = n_docs/2
    n_docs.times do |i|
      @@test.insert(:foo => i)
    end

    cursor = @@test.find({}, :batch_size => batch_size, :limit => -7)
    cursor.next
    assert_equal n_docs, cursor.instance_variable_get(:@returned)
  end

  def test_limit_and_skip
    10.times do |i|
      @@test.save(:foo => i)
    end

    assert_equal 5, @@test.find({}, :skip => 5).next_document()["foo"]
    assert_equal nil, @@test.find({}, :skip => 10).next_document()

    assert_equal 5, @@test.find({}, :limit => 5).to_a.length

    assert_equal 3, @@test.find({}, :skip => 3, :limit => 5).next_document()["foo"]
    assert_equal 5, @@test.find({}, :skip => 3, :limit => 5).to_a.length
  end

  def test_large_limit
    2000.times do |i|
      @@test.insert("x" => i, "y" => "mongomongo" * 1000)
    end

    assert_equal 2000, @@test.count

    i = 0
    y = 0
    @@test.find({}, :limit => 1900).each do |doc|
      i += 1
      y += doc["x"]
    end

    assert_equal 1900, i
    assert_equal 1804050, y
  end

  def test_small_limit
    @@test.insert("x" => "hello world")
    @@test.insert("x" => "goodbye world")

    assert_equal 2, @@test.count

    x = 0
    @@test.find({}, :limit => 1).each do |doc|
      x += 1
      assert_equal "hello world", doc["x"]
    end

    assert_equal 1, x
  end

  def test_find_with_transformer
    klass       = Struct.new(:id, :a)
    transformer = Proc.new { |doc| klass.new(doc['_id'], doc['a']) }
    cursor      = @@test.find({}, :transformer => transformer)
    assert_equal(transformer, cursor.transformer)
  end

  def test_find_one_with_transformer
    klass       = Struct.new(:id, :a)
    transformer = Proc.new { |doc| klass.new(doc['_id'], doc['a']) }
    id          = @@test.insert('a' => 1)
    doc         = @@test.find_one(id, :transformer => transformer)
    assert_instance_of(klass, doc)
  end

  def test_ensure_index
    @@test.drop_indexes
    @@test.insert("x" => "hello world")
    assert_equal 1, @@test.index_information.keys.count #default index

    @@test.ensure_index([["x", Mongo::DESCENDING]], {})
    assert_equal 2, @@test.index_information.keys.count
    assert @@test.index_information.keys.include?("x_-1")

    @@test.ensure_index([["x", Mongo::ASCENDING]])
    assert @@test.index_information.keys.include?("x_1")

    @@test.ensure_index([["type", 1], ["date", -1]])
    assert @@test.index_information.keys.include?("type_1_date_-1")

    @@test.drop_index("x_1")
    assert_equal 3, @@test.index_information.keys.count
    @@test.drop_index("x_-1")
    assert_equal 2, @@test.index_information.keys.count

    @@test.ensure_index([["x", Mongo::DESCENDING]], {})
    assert_equal 3, @@test.index_information.keys.count
    assert @@test.index_information.keys.include?("x_-1")

    # Make sure that drop_index expires cache properly
    @@test.ensure_index([['a', 1]])
    assert @@test.index_information.keys.include?("a_1")
    @@test.drop_index("a_1")
    assert !@@test.index_information.keys.include?("a_1")
    @@test.ensure_index([['a', 1]])
    assert @@test.index_information.keys.include?("a_1")
    @@test.drop_index("a_1")
    @@test.drop_indexes
  end

  def test_ensure_index_timeout
    @@db.cache_time = 1
    coll = @@db['ensure_test']
    coll.expects(:generate_indexes).twice
    coll.ensure_index([['a', 1]])

    # These will be cached
    coll.ensure_index([['a', 1]])
    coll.ensure_index([['a', 1]])
    coll.ensure_index([['a', 1]])
    coll.ensure_index([['a', 1]])

    sleep(1)
    # This won't be, so generate_indexes will be called twice
    coll.ensure_index([['a', 1]])
    coll.drop
  end

  if @@version > '2.0.0'
    def test_show_disk_loc
      @@test.save({:a => 1})
      @@test.save({:a => 2})
      assert @@test.find({:a => 1}, :show_disk_loc => true).show_disk_loc
      assert @@test.find({:a => 1}, :show_disk_loc => true).next['$diskLoc']
      @@test.remove
    end

    def test_max_scan
      @@test.drop
      n = 100
      n.times do |i|
        @@test.save({:_id => i, :x => i % 10})
      end
      assert_equal(n, @@test.find.to_a.size)
      assert_equal(50, @@test.find({}, :max_scan => 50).to_a.size)
      assert_equal(10, @@test.find({:x => 2}).to_a.size)
      assert_equal(5, @@test.find({:x => 2}, :max_scan => 50).to_a.size)
      @@test.ensure_index([[:x, 1]])
      assert_equal(10, @@test.find({:x => 2}, :max_scan => n).to_a.size)
      @@test.drop
    end
  end

  context "Grouping" do
    setup do
      @@test.remove
      @@test.save("a" => 1)
      @@test.save("b" => 1)
      @initial = {"count" => 0}
      @reduce_function = "function (obj, prev) { prev.count += inc_value; }"
    end

    should "fail if missing required options" do
      assert_raise MongoArgumentError do
        @@test.group(:initial => {})
      end

      assert_raise MongoArgumentError do
        @@test.group(:reduce => "foo")
      end
    end

    should "group results using eval form" do
      assert_equal 1, @@test.group(:initial => @initial, :reduce => Code.new(@reduce_function, {"inc_value" => 0.5}))[0]["count"]
      assert_equal 2, @@test.group(:initial => @initial, :reduce => Code.new(@reduce_function, {"inc_value" => 1}))[0]["count"]
      assert_equal 4, @@test.group(:initial => @initial, :reduce => Code.new(@reduce_function, {"inc_value" => 2}))[0]["count"]
    end

    should "finalize grouped results" do
      @finalize = "function(doc) {doc.f = doc.count + 200; }"
      assert_equal 202, @@test.group(:initial => @initial, :reduce => Code.new(@reduce_function, {"inc_value" => 1}), :finalize => @finalize)[0]["f"]
    end
  end

  context "Grouping with key" do
    setup do
      @@test.remove
      @@test.save("a" => 1, "pop" => 100)
      @@test.save("a" => 1, "pop" => 100)
      @@test.save("a" => 2, "pop" => 100)
      @@test.save("a" => 2, "pop" => 100)
      @initial = {"count" => 0, "foo" => 1}
      @reduce_function = "function (obj, prev) { prev.count += obj.pop; }"
    end

    should "group" do
      result = @@test.group(:key => :a, :initial => @initial, :reduce => @reduce_function)
      assert result.all? { |r| r['count'] == 200 }
    end
  end

  context "Grouping with a key function" do
    setup do
      @@test.remove
      @@test.save("a" => 1)
      @@test.save("a" => 2)
      @@test.save("a" => 3)
      @@test.save("a" => 4)
      @@test.save("a" => 5)
      @initial = {"count" => 0}
      @keyf    = "function (doc) { if(doc.a % 2 == 0) { return {even: true}; } else {return {odd: true}} };"
      @reduce  = "function (obj, prev) { prev.count += 1; }"
    end

    should "group results" do
      results = @@test.group(:keyf => @keyf, :initial => @initial, :reduce => @reduce).sort {|a, b| a['count'] <=> b['count']}
      assert results[0]['even'] && results[0]['count'] == 2.0
      assert results[1]['odd'] && results[1]['count'] == 3.0
    end

    should "group filtered results" do
      results = @@test.group(:keyf => @keyf, :cond => {:a => {'$ne' => 2}},
        :initial => @initial, :reduce => @reduce).sort {|a, b| a['count'] <=> b['count']}
      assert results[0]['even'] && results[0]['count'] == 1.0
      assert results[1]['odd'] && results[1]['count'] == 3.0
    end
  end

  context "A collection with two records" do
    setup do
      @collection = @@db.collection('test-collection')
      @collection.remove
      @collection.insert({:name => "Jones"})
      @collection.insert({:name => "Smith"})
    end

    should "have two records" do
      assert_equal 2, @collection.size
    end

    should "remove the two records" do
      @collection.remove()
      assert_equal 0, @collection.size
    end

    should "remove all records if an empty document is specified" do
      @collection.remove({})
      assert_equal 0, @collection.find.count
    end

    should "remove only matching records" do
      @collection.remove({:name => "Jones"})
      assert_equal 1, @collection.size
    end
  end

  context "Drop index " do
    setup do
      @@db.drop_collection('test-collection')
      @collection = @@db.collection('test-collection')
    end

    should "drop an index" do
      @collection.create_index([['a', Mongo::ASCENDING]])
      assert @collection.index_information['a_1']
      @collection.drop_index([['a', Mongo::ASCENDING]])
      assert_nil @collection.index_information['a_1']
    end

    should "drop an index which was given a specific name" do
      @collection.create_index([['a', Mongo::DESCENDING]], {:name => 'i_will_not_fear'})
      assert @collection.index_information['i_will_not_fear']
      @collection.drop_index([['a', Mongo::DESCENDING]])
      assert_nil @collection.index_information['i_will_not_fear']
    end

    should "drops an composite index" do
      @collection.create_index([['a', Mongo::DESCENDING], ['b', Mongo::ASCENDING]])
      assert @collection.index_information['a_-1_b_1']
      @collection.drop_index([['a', Mongo::DESCENDING], ['b', Mongo::ASCENDING]])
      assert_nil @collection.index_information['a_-1_b_1']
    end

    should "drops an index with symbols" do
      @collection.create_index([['a', Mongo::DESCENDING], [:b, Mongo::ASCENDING]])
      assert @collection.index_information['a_-1_b_1']
      @collection.drop_index([['a', Mongo::DESCENDING], [:b, Mongo::ASCENDING]])
      assert_nil @collection.index_information['a_-1_b_1']
    end
  end

  context "Creating indexes " do
    setup do
      @@db.drop_collection('geo')
      @@db.drop_collection('test-collection')
      @collection = @@db.collection('test-collection')
      @geo        = @@db.collection('geo')
    end

    should "create index using symbols" do
      @collection.create_index :foo, :name => :bar
      @geo.create_index :goo, :name => :baz
      assert @collection.index_information['bar']
      @collection.drop_index :bar
      assert_nil @collection.index_information['bar']
      assert @geo.index_information['baz']
      @geo.drop_index(:baz)
      assert_nil @geo.index_information['baz']
    end

    #should "create a text index" do
    #  @geo.save({'title' => "some text"})
    #  @geo.create_index([['title', Mongo::TEXT]])
    #  assert @geo.index_information['title_text']
    #end

    should "create a hashed index" do
      @geo.save({'a' => 1})
      @geo.create_index([['a', Mongo::HASHED]])
      assert @geo.index_information['a_hashed']
    end

    should "create a geospatial index" do
      @geo.save({'loc' => [-100, 100]})
      @geo.create_index([['loc', Mongo::GEO2D]])
      assert @geo.index_information['loc_2d']
    end

    should "create a geoHaystack index" do
      @geo.save({ "_id" => 100, "pos" => { "long" => 126.9, "lat" => 35.2 }, "type" => "restaurant"})
      @geo.create_index([['pos', Mongo::GEOHAYSTACK], ['type', Mongo::ASCENDING]], :bucket_size => 1)
      assert @geo.index_information['pos_geoHaystack_type_1']
    end

    should "create a geo 2dsphere index" do
      @collection.insert({"coordinates" => [ 5 , 5 ], "type" => "Point"})
      @geo.create_index([['coordinates', Mongo::GEO2DSPHERE]])
      assert @geo.index_information['coordinates_2dsphere']
    end

    should "create a unique index" do
      @collection.create_index([['a', Mongo::ASCENDING]], :unique => true)
      assert @collection.index_information['a_1']['unique'] == true
    end

    should "drop duplicates" do
      @collection.insert({:a => 1})
      @collection.insert({:a => 1})
      assert_equal 2, @collection.find({:a => 1}).count
      @collection.create_index([['a', Mongo::ASCENDING]], :unique => true, :dropDups => true)
      assert_equal 1, @collection.find({:a => 1}).count
    end

    should "drop duplicates with ruby-like drop_dups key" do
      @collection.insert({:a => 1})
      @collection.insert({:a => 1})
      assert_equal 2, @collection.find({:a => 1}).count
      @collection.create_index([['a', Mongo::ASCENDING]], :unique => true, :drop_dups => true)
      assert_equal 1, @collection.find({:a => 1}).count
    end

    should "drop duplicates with ensure_index and drop_dups key" do
      @collection.insert({:a => 1})
      @collection.insert({:a => 1})
      assert_equal 2, @collection.find({:a => 1}).count
      @collection.ensure_index([['a', Mongo::ASCENDING]], :unique => true, :drop_dups => true)
      assert_equal 1, @collection.find({:a => 1}).count
    end

    should "create an index in the background" do
      if @@version > '1.3.1'
        @collection.create_index([['b', Mongo::ASCENDING]], :background => true)
        assert @collection.index_information['b_1']['background'] == true
      else
        assert true
      end
    end

    should "require an array of arrays" do
      assert_raise MongoArgumentError do
        @collection.create_index(['c', Mongo::ASCENDING])
      end
    end

    should "enforce proper index types" do
      assert_raise MongoArgumentError do
        @collection.create_index([['c', 'blah']])
      end
    end

    should "raise an error if index name is greater than 128" do
      assert_raise Mongo::OperationFailure do
        @collection.create_index([['a' * 25, 1], ['b' * 25, 1],
          ['c' * 25, 1], ['d' * 25, 1], ['e' * 25, 1]])
      end
    end

    should "allow for an alternate name to be specified" do
      @collection.create_index([['a' * 25, 1], ['b' * 25, 1],
        ['c' * 25, 1], ['d' * 25, 1], ['e' * 25, 1]], :name => 'foo_index')
      assert @collection.index_information['foo_index']
    end

    should "generate indexes in the proper order" do
      key = BSON::OrderedHash['b', 1, 'a', 1]
      if @@version < '2.5.5'
        @collection.expects(:send_write) do |type, selector, documents, check_keys, opts, collection_name|
          assert_equal key, selector[:key]
        end
      else
        @collection.db.expects(:command) do |selector|
          assert_equal key, selector[:indexes].first[:key]
        end
      end
      @collection.create_index([['b', 1], ['a', 1]])
    end

    should "allow creation of multiple indexes" do
      assert @collection.create_index([['a', 1]])
      assert @collection.create_index([['a', 1]])
    end

    context "with an index created" do
      setup do
        @collection.create_index([['b', 1], ['a', 1]])
      end

      should "return properly ordered index information" do
        assert @collection.index_information['b_1_a_1']
      end
    end
  end

  context "Capped collections" do
    setup do
      @@db.drop_collection('log')
      @capped = @@db.create_collection('log', :capped => true, :size => LIMITED_MAX_BSON_SIZE)

      10.times { |n| @capped.insert({:n => n}) }
    end

    should "find using a standard cursor" do
      cursor = @capped.find
      10.times do
        assert cursor.next_document
      end
      assert_nil cursor.next_document
      @capped.insert({:n => 100})
      assert_nil cursor.next_document
    end

    should "fail tailable cursor on a non-capped collection" do
      col = @@db['regular-collection']
      col.insert({:a => 1000})
      tail = Cursor.new(col, :tailable => true, :order => [['$natural', 1]])
      assert_raise OperationFailure do
        tail.next_document
      end
    end

    should "find using a tailable cursor" do
      tail = Cursor.new(@capped, :tailable => true, :order => [['$natural', 1]])
      10.times do
        assert tail.next_document
      end
      assert_nil tail.next_document
      @capped.insert({:n => 100})
      assert tail.next_document
    end
  end
end
