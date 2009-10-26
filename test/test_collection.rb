# --
# Copyright (C) 2008-2009 10gen Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ++

require 'test/test_helper'
class TestCollection < Test::Unit::TestCase
  @@connection = Connection.new(ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost', ENV['MONGO_RUBY_DRIVER_PORT'] || Connection::DEFAULT_PORT)
  @@db   = @@connection.db('ruby-mongo-test')
  @@test = @@db.collection("test")
  @@version = @@connection.server_version

  def setup
    @@test.drop()
  end

  def test_optional_pk_factory
    @coll_default_pk = @@db.collection('stuff')
    assert_equal Mongo::ObjectID, @coll_default_pk.pk_factory
    @coll_default_pk = @@db.create_collection('more-stuff')
    assert_equal Mongo::ObjectID, @coll_default_pk.pk_factory

    # Create a db with a pk_factory.
    @db = Connection.new(ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost',
                         ENV['MONGO_RUBY_DRIVER_PORT'] || Connection::DEFAULT_PORT).db('ruby-mongo-test', :pk => Object.new)
    @coll = @db.collection('coll-with-pk')
    assert @coll.pk_factory.is_a?(Object)

    @coll = @db.create_collection('created_coll_with_pk')
    assert @coll.pk_factory.is_a?(Object)
  end

  def test_collection
    assert_raise InvalidName do
      @@db["te$t"]
    end

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

  def test_safe_insert
    a = {"hello" => "world"}
    @@test.insert(a)
    @@test.insert(a)
    assert(@@db.error.include?("E11000"))

    assert_raise OperationFailure do
      @@test.insert(a, :safe => true)
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
        @@test.update({}, {"$inc" => {"x" => 1}}, :safe => true)
      end
    end
  else
    def test_safe_update
      @@test.create_index("x", true)
      @@test.insert("x" => 5)
      @@test.insert("x" => 10)

      # Can update an indexed collection.
      @@test.update({}, {"$inc" => {"x" => 1}})
      assert !@@db.error?

      # Can't duplicate an index.
      assert_raise OperationFailure do
        @@test.update({}, {"x" => 10}, :safe => true, :upsert => true)
      end
    end
  end

  def test_safe_save
    @@test.create_index("hello", true)

    @@test.save("hello" => "world")
    @@test.save("hello" => "world")
    assert(@@db.error.include?("E11000"))

    assert_raise OperationFailure do
      @@test.save({"hello" => "world"}, :safe => true)
    end
  end

  def test_count
    @@test.drop

    assert_equal 0, @@test.count
    @@test.save("x" => 1)
    @@test.save("x" => 2)
    assert_equal 2, @@test.count
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

  def test_find_one
    id = @@test.save("hello" => "world", "foo" => "bar")

    assert_equal "world", @@test.find_one()["hello"]
    assert_equal @@test.find_one(id), @@test.find_one()
    assert_equal @@test.find_one(nil), @@test.find_one()
    assert_equal @@test.find_one({}), @@test.find_one()
    assert_equal @@test.find_one("hello" => "world"), @@test.find_one()
    assert_equal @@test.find_one(OrderedHash["hello", "world"]), @@test.find_one()

    assert @@test.find_one(nil, :fields => ["hello"]).include?("hello")
    assert !@@test.find_one(nil, :fields => ["foo"]).include?("hello")
    assert_equal ["_id"], @@test.find_one(nil, :fields => []).keys()

    assert_equal nil, @@test.find_one("hello" => "foo")
    assert_equal nil, @@test.find_one(OrderedHash["hello", "foo"])
    assert_equal nil, @@test.find_one(ObjectID.new)

    assert_raise TypeError do
      @@test.find_one(6)
    end
  end

  def test_insert_adds_id
    doc = {"hello" => "world"}
    @@test.insert(doc)
    assert(doc.include?(:_id))

    docs = [{"hello" => "world"}, {"hello" => "world"}]
    @@test.insert(docs)
    docs.each do |doc|
      assert(doc.include?(:_id))
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

  def test_saving_dates_pre_epoch
    begin
      @@test.save({'date' => Time.utc(1600)})
      assert_in_delta Time.utc(1600), @@test.find_one()["date"], 0.001
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

  def test_limit_and_skip
    10.times do |i|
      @@test.save(:foo => i)
    end

    # TODO remove test for deprecated :offset option
    assert_equal 5, @@test.find({}, :offset => 5).next_object()["foo"]

    assert_equal 5, @@test.find({}, :skip => 5).next_object()["foo"]
    assert_equal nil, @@test.find({}, :skip => 10).next_object()

    assert_equal 5, @@test.find({}, :limit => 5).to_a.length

    assert_equal 3, @@test.find({}, :skip => 3, :limit => 5).next_object()["foo"]
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

  def test_group_with_scope
    @@test.save("a" => 1)
    @@test.save("b" => 1)

    reduce_function = "function (obj, prev) { prev.count += inc_value; }"

    assert_equal 2, @@test.group([], {}, {"count" => 0},
                                 Code.new(reduce_function,
                                          {"inc_value" => 1}))[0]["count"]

# TODO enable these tests when SERVER-262 is fixed

#     assert_equal 2, @@test.group([], {}, {"count" => 0},
#                                  Code.new(reduce_function,
#                                           {"inc_value" => 1}), true)[0]["count"]

    assert_equal 4, @@test.group([], {}, {"count" => 0},
                                 Code.new(reduce_function,
                                          {"inc_value" => 2}))[0]["count"]
#     assert_equal 4, @@test.group([], {}, {"count" => 0},
#                                  Code.new(reduce_function,
#                                           {"inc_value" => 2}), true)[0]["count"]

    assert_equal 1, @@test.group([], {}, {"count" => 0},
                                 Code.new(reduce_function,
                                          {"inc_value" => 0.5}))[0]["count"]
#     assert_equal 1, @@test.group([], {}, {"count" => 0},
#                                  Code.new(reduce_function,
#                                           {"inc_value" => 0.5}), true)[0]["count"]
    end

  context "A collection with two records" do 
    setup do 
      @collection = @@db.collection('test-collection')
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

    should "remove all records if deprecated clear is used" do 
      @collection.clear
      assert_equal 0, @collection.find.count
    end

    should "remove only matching records" do 
      @collection.remove({:name => "Jones"})
      assert_equal 1, @collection.size
    end
  end
end
