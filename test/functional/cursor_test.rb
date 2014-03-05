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

require 'test_helper'
require 'logger'

class CursorTest < Test::Unit::TestCase
  include Mongo
  include Mongo::Constants

  @@connection = standard_connection
  @@db   = @@connection.db(TEST_DB)
  @@coll = @@db.collection('test')
  @@version = @@connection.server_version

  def setup
    @@coll.remove
    @@coll.insert('a' => 1)     # collection not created until it's used
    @@coll_full_name = "#{TEST_DB}.test"
  end

  def test_alive
    batch = []
    5000.times do |n|
      batch << {:a => n}
    end

    @@coll.insert(batch)
    cursor = @@coll.find
    assert !cursor.alive?
    cursor.next
    assert cursor.alive?
    cursor.close
    assert !cursor.alive?
    @@coll.remove
  end

  def test_add_and_remove_options
    c = @@coll.find
    assert_equal 0, c.options & OP_QUERY_EXHAUST
    c.add_option(OP_QUERY_EXHAUST)
    assert_equal OP_QUERY_EXHAUST, c.options & OP_QUERY_EXHAUST
    c.remove_option(OP_QUERY_EXHAUST)
    assert_equal 0, c.options & OP_QUERY_EXHAUST

    c.next
    assert_raise Mongo::InvalidOperation do
      c.add_option(OP_QUERY_EXHAUST)
    end

    assert_raise Mongo::InvalidOperation do
      c.add_option(OP_QUERY_EXHAUST)
    end
  end

  def test_exhaust
    if @@version >= "2.0"
      @@coll.remove
      data = "1" * 10_000
      5000.times do |n|
        @@coll.insert({:n => n, :data => data})
      end

      c = Cursor.new(@@coll)
      c.add_option(OP_QUERY_EXHAUST)
      assert_equal @@coll.count, c.to_a.size
      assert c.closed?

      c = Cursor.new(@@coll)
      c.add_option(OP_QUERY_EXHAUST)
      4999.times do
        c.next
      end
      assert c.has_next?
      assert c.next
      assert !c.has_next?
      assert c.closed?

      @@coll.remove
    end
  end

  def test_compile_regex_get_more
    return unless defined?(BSON::BSON_RUBY) && BSON::BSON_CODER == BSON::BSON_RUBY
    @@coll.remove
    n_docs = 3
    n_docs.times { |n| @@coll.insert({ 'n' => /.*/ }) }
    cursor = @@coll.find({}, :batch_size => (n_docs-1), :compile_regex => false)
    cursor.expects(:send_get_more)
    cursor.to_a.each do |doc|
      assert_kind_of BSON::Regex, doc['n']
    end
  end

  def test_max_time_ms_error
    cursor = @@coll.find
    cursor.stubs(:send_initial_query).returns(true)

    cursor.instance_variable_set(:@cache, [{
      '$err' => 'operation exceeded time limit',
      'code' => 50
    }])

    assert_raise ExecutionTimeout do
      cursor.to_a
    end
  end

  def test_max_time_ms
    with_forced_timeout(@@connection) do
      assert_raise ExecutionTimeout do
        cursor = @@coll.find.max_time_ms(100)
        cursor.to_a
      end
    end
  end

  def test_exhaust_after_limit_error
    c = Cursor.new(@@coll, :limit => 17)
    assert_raise MongoArgumentError do
      c.add_option(OP_QUERY_EXHAUST)
    end

    assert_raise MongoArgumentError do
      c.add_option(OP_QUERY_EXHAUST + OP_QUERY_SLAVE_OK)
    end
  end

  def test_limit_after_exhaust_error
    c = Cursor.new(@@coll)
    c.add_option(OP_QUERY_EXHAUST)
    assert_raise MongoArgumentError do
      c.limit(17)
    end
  end

  def test_exhaust_with_mongos
    @@connection.expects(:mongos?).returns(:true)
    c = Cursor.new(@@coll)

    assert_raise MongoArgumentError do
      c.add_option(OP_QUERY_EXHAUST)
    end
  end

  def test_inspect
    selector = {:a => 1}
    cursor = @@coll.find(selector)
    assert_equal "<Mongo::Cursor:0x#{cursor.object_id.to_s(16)} namespace='#{@@db.name}.#{@@coll.name}' " +
        "@selector=#{selector.inspect} @cursor_id=#{cursor.cursor_id}>", cursor.inspect
  end

  def test_explain
    cursor = @@coll.find('a' => 1)
    explaination = cursor.explain
    assert_not_nil explaination['cursor']
    assert_kind_of Numeric, explaination['n']
    assert_kind_of Numeric, explaination['millis']
    assert_kind_of Numeric, explaination['nscanned']
  end

  def test_each_with_no_block
    assert_kind_of(Enumerator, @@coll.find().each) if defined? Enumerator
  end

  def test_count
    @@coll.remove

    assert_equal 0, @@coll.find().count()

    10.times do |i|
      @@coll.save("x" => i)
    end

    assert_equal 10, @@coll.find().count()
    assert_kind_of Integer, @@coll.find().count()
    assert_equal 10, @@coll.find({}, :limit => 5).count()
    assert_equal 10, @@coll.find({}, :skip => 5).count()

    assert_equal 5, @@coll.find({}, :limit => 5).count(true)
    assert_equal 5, @@coll.find({}, :skip => 5).count(true)
    assert_equal 2, @@coll.find({}, :skip => 5, :limit => 2).count(true)

    assert_equal 1, @@coll.find({"x" => 1}).count()
    assert_equal 5, @@coll.find({"x" => {"$lt" => 5}}).count()

    a = @@coll.find()
    b = a.count()
    a.each do |doc|
      break
    end
    assert_equal b, a.count()

    assert_equal 0, @@db['acollectionthatdoesn'].count()
  end

  def test_sort
    @@coll.remove
    5.times{|x| @@coll.insert({"age" => x}) }

    assert_kind_of Cursor, @@coll.find().sort(:age, 1)

    assert_equal 0, @@coll.find().sort(:age, 1).next_document["age"]
    assert_equal 4, @@coll.find().sort(:age, -1).next_document["age"]
    assert_equal 0, @@coll.find().sort([["age", :asc]]).next_document["age"]

    assert_kind_of Cursor, @@coll.find().sort([[:age, -1], [:b, 1]])

    assert_equal 4, @@coll.find().sort(:age, 1).sort(:age, -1).next_document["age"]
    assert_equal 0, @@coll.find().sort(:age, -1).sort(:age, 1).next_document["age"]

    assert_equal 4, @@coll.find().sort([:age, :asc]).sort(:age, -1).next_document["age"]
    assert_equal 0, @@coll.find().sort([:age, :desc]).sort(:age, 1).next_document["age"]

    cursor = @@coll.find()
    cursor.next_document
    assert_raise InvalidOperation do
      cursor.sort(["age"])
    end

    assert_raise InvalidSortValueError do
      @@coll.find().sort(:age, 25).next_document
    end

    assert_raise InvalidSortValueError do
      @@coll.find().sort(25).next_document
    end
  end

  def test_sort_date
    @@coll.remove
    5.times{|x| @@coll.insert({"created_at" => Time.utc(2000 + x)}) }

    assert_equal 2000, @@coll.find().sort(:created_at, :asc).next_document["created_at"].year
    assert_equal 2004, @@coll.find().sort(:created_at, :desc).next_document["created_at"].year

    assert_equal 2000, @@coll.find().sort([:created_at, :asc]).next_document["created_at"].year
    assert_equal 2004, @@coll.find().sort([:created_at, :desc]).next_document["created_at"].year

    assert_equal 2000, @@coll.find().sort([[:created_at, :asc]]).next_document["created_at"].year
    assert_equal 2004, @@coll.find().sort([[:created_at, :desc]]).next_document["created_at"].year
  end

  def test_sort_min_max_keys
    @@coll.remove
    @@coll.insert({"n" => 1000000})
    @@coll.insert({"n" => -1000000})
    @@coll.insert({"n" => MaxKey.new})
    @@coll.insert({"n" => MinKey.new})

    results = @@coll.find.sort([:n, :asc]).to_a

    assert_equal MinKey.new, results[0]['n']
    assert_equal(-1000000,   results[1]['n'])
    assert_equal 1000000,    results[2]['n']
    assert_equal MaxKey.new, results[3]['n']
  end

  def test_id_range_queries
    @@coll.remove

    t1 = Time.now
    t1_id = ObjectId.from_time(t1)
    @@coll.save({:t => 't1'})
    @@coll.save({:t => 't1'})
    @@coll.save({:t => 't1'})
    sleep(1)
    t2 = Time.now
    t2_id = ObjectId.from_time(t2)
    @@coll.save({:t => 't2'})
    @@coll.save({:t => 't2'})
    @@coll.save({:t => 't2'})

    assert_equal 3, @@coll.find({'_id' => {'$gt' => t1_id, '$lt' => t2_id}}).count
    @@coll.find({'_id' => {'$gt' => t2_id}}).each do |doc|
      assert_equal 't2', doc['t']
    end
  end

  def test_limit
    @@coll.remove

    10.times do |i|
      @@coll.save("x" => i)
    end
    assert_equal 10, @@coll.find().count()

    results = @@coll.find().limit(5).to_a
    assert_equal 5, results.length
  end

  def test_timeout_options
    cursor = Cursor.new(@@coll)
    assert_equal true, cursor.timeout

    cursor = @@coll.find
    assert_equal true, cursor.timeout

    cursor = @@coll.find({}, :timeout => nil)
    assert_equal true, cursor.timeout

    cursor = Cursor.new(@@coll, :timeout => false)
    assert_equal false, cursor.timeout

    @@coll.find({}, :timeout => false) do |c|
      assert_equal false, c.timeout
    end
  end

  def test_timeout
    opts = Cursor.new(@@coll).options
    assert_equal 0, opts & Mongo::Constants::OP_QUERY_NO_CURSOR_TIMEOUT

    opts = Cursor.new(@@coll, :timeout => false).options
    assert_equal Mongo::Constants::OP_QUERY_NO_CURSOR_TIMEOUT,
      opts & Mongo::Constants::OP_QUERY_NO_CURSOR_TIMEOUT
  end

  def test_limit_exceptions
    cursor      = @@coll.find()
    cursor.next_document
    assert_raise InvalidOperation, "Cannot modify the query once it has been run or closed." do
      cursor.limit(1)
    end

    cursor = @@coll.find()
    cursor.close
    assert_raise InvalidOperation, "Cannot modify the query once it has been run or closed." do
      cursor.limit(1)
    end
  end

  def test_skip
    @@coll.remove

    10.times do |i|
      @@coll.save("x" => i)
    end
    assert_equal 10, @@coll.find().count()

    all_results    = @@coll.find().to_a
    skip_results = @@coll.find().skip(2).to_a
    assert_equal 10, all_results.length
    assert_equal 8,  skip_results.length

    assert_equal all_results.slice(2...10), skip_results
  end

  def test_skip_exceptions
    cursor      = @@coll.find()
    cursor.next_document
    assert_raise InvalidOperation, "Cannot modify the query once it has been run or closed." do
      cursor.skip(1)
    end

    cursor = @@coll.find()
    cursor.close
    assert_raise InvalidOperation, "Cannot modify the query once it has been run or closed." do
      cursor.skip(1)
    end
  end

  def test_limit_skip_chaining
    @@coll.remove
    10.times do |i|
      @@coll.save("x" => i)
    end

    all_results = @@coll.find().to_a
    limited_skip_results = @@coll.find().limit(5).skip(3).to_a

    assert_equal all_results.slice(3...8), limited_skip_results
  end

  def test_close_no_query_sent
    begin
      cursor = @@coll.find('a' => 1)
      cursor.close
      assert cursor.closed?
    rescue => ex
      fail ex.to_s
    end
  end

  def test_refill_via_get_more
    assert_equal 1, @@coll.count
    1000.times { |i|
      assert_equal 1 + i, @@coll.count
      @@coll.insert('a' => i)
    }

    assert_equal 1001, @@coll.count
    count = 0
    @@coll.find.each { |obj|
      count += obj['a']
    }
    assert_equal 1001, @@coll.count

    # do the same thing again for debugging
    assert_equal 1001, @@coll.count
    count2 = 0
    @@coll.find.each { |obj|
      count2 += obj['a']
    }
    assert_equal 1001, @@coll.count

    assert_equal count, count2
    assert_equal 499501, count
  end

  def test_refill_via_get_more_alt_coll
    coll = @@db.collection('test-alt-coll')
    coll.remove
    coll.insert('a' => 1)     # collection not created until it's used
    assert_equal 1, coll.count

    1000.times { |i|
      assert_equal 1 + i, coll.count
      coll.insert('a' => i)
    }

    assert_equal 1001, coll.count
    count = 0
    coll.find.each { |obj|
      count += obj['a']
    }
    assert_equal 1001, coll.count

    # do the same thing again for debugging
    assert_equal 1001, coll.count
    count2 = 0
    coll.find.each { |obj|
      count2 += obj['a']
    }
    assert_equal 1001, coll.count

    assert_equal count, count2
    assert_equal 499501, count
  end

  def test_close_after_query_sent
    begin
      cursor = @@coll.find('a' => 1)
      cursor.next_document
      cursor.close
      assert cursor.closed?
    rescue => ex
      fail ex.to_s
    end
  end

  def test_kill_cursors
    @@coll.drop

    client_cursors = @@db.command("cursorInfo" => 1)["clientCursors_size"]

    10000.times do |i|
      @@coll.insert("i" => i)
    end

    assert_equal(client_cursors,
                 @@db.command("cursorInfo" => 1)["clientCursors_size"])

    10.times do |i|
      @@coll.find_one()
    end

    assert_equal(client_cursors,
                 @@db.command("cursorInfo" => 1)["clientCursors_size"])

    10.times do |i|
      a = @@coll.find()
      a.next_document
      a.close()
    end

    assert_equal(client_cursors,
                 @@db.command("cursorInfo" => 1)["clientCursors_size"])

    a = @@coll.find()
    a.next_document

    assert_not_equal(client_cursors,
                     @@db.command("cursorInfo" => 1)["clientCursors_size"])

    a.close()

    assert_equal(client_cursors,
                 @@db.command("cursorInfo" => 1)["clientCursors_size"])

    a = @@coll.find({}, :limit => 10).next_document

    assert_equal(client_cursors,
                 @@db.command("cursorInfo" => 1)["clientCursors_size"])

    @@coll.find() do |cursor|
      cursor.next_document
    end

    assert_equal(client_cursors,
                 @@db.command("cursorInfo" => 1)["clientCursors_size"])

    @@coll.find() { |cursor|
      cursor.next_document
    }

    assert_equal(client_cursors,
                 @@db.command("cursorInfo" => 1)["clientCursors_size"])
  end

  def test_count_with_fields
    @@coll.remove
    @@coll.save("x" => 1)

    if @@version < "1.1.3"
      assert_equal(0, @@coll.find({}, :fields => ["a"]).count())
    else
      assert_equal(1, @@coll.find({}, :fields => ["a"]).count())
    end
  end

  def test_has_next
    @@coll.remove
    200.times do |n|
      @@coll.save("x" => n)
    end

    cursor = @@coll.find
    n = 0
    while cursor.has_next?
      assert cursor.next
      n += 1
    end

    assert_equal n, 200
    assert_equal false, cursor.has_next?
  end

  def test_cursor_invalid
    @@coll.remove
    10000.times do |n|
      @@coll.insert({:a => n})
    end

    cursor = @@coll.find({})

    assert_raise_error Mongo::OperationFailure, "CURSOR_NOT_FOUND" do
      9999.times do
        cursor.next_document
        cursor.instance_variable_set(:@cursor_id, 1234567890)
      end
    end
  end

  def test_enumberables
    @@coll.remove
    100.times do |n|
      @@coll.insert({:a => n})
    end

    assert_equal 100, @@coll.find.to_a.length
    assert_equal 100, @@coll.find.to_set.length

    cursor = @@coll.find
    50.times { |n| cursor.next_document }
    assert_equal 50, cursor.to_a.length
  end

  def test_rewind
    @@coll.remove
    100.times do |n|
      @@coll.insert({:a => n})
    end

    cursor = @@coll.find
    cursor.to_a
    assert_equal [], cursor.map {|doc| doc }

    cursor.rewind!
    assert_equal 100, cursor.map {|doc| doc }.length

    cursor.rewind!
    5.times { cursor.next_document }
    cursor.rewind!
    assert_equal 100, cursor.map {|doc| doc }.length
  end

  def test_transformer
    transformer = Proc.new { |doc| doc }
    cursor = Cursor.new(@@coll, :transformer => transformer)
    assert_equal(transformer, cursor.transformer)
  end

  def test_instance_transformation_with_next
    klass       = Struct.new(:id, :a)
    transformer = Proc.new { |doc| klass.new(doc['_id'], doc['a']) }
    cursor      = Cursor.new(@@coll, :transformer => transformer)
    instance    = cursor.next

    assert_instance_of(klass, instance)
    assert_instance_of(BSON::ObjectId, instance.id)
    assert_equal(1, instance.a)
  end

  def test_instance_transformation_with_each
    klass       = Struct.new(:id, :a)
    transformer = Proc.new { |doc| klass.new(doc['_id'], doc['a']) }
    cursor      = Cursor.new(@@coll, :transformer => transformer)

    cursor.each do |instance|
      assert_instance_of(klass, instance)
    end
  end
end
