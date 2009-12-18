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

  def test_valid_names
    assert_raise InvalidName do
      @@db["te$t"]
    end

    assert_raise InvalidName do
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

  def test_nil_id
    assert_equal 5, @@test.insert({"_id" => 5, "foo" => "bar"}, {:safe => true})
    assert_equal 5, @@test.save({"_id" => 5, "foo" => "baz"}, {:safe => true})
    assert_equal nil, @@test.find_one("foo" => "bar")
    assert_equal "baz", @@test.find_one(:_id => 5)["foo"]
    assert_raise OperationFailure do
      @@test.insert({"_id" => 5, "foo" => "bar"}, {:safe => true})
    end

    assert_equal nil, @@test.insert({"_id" => nil, "foo" => "bar"}, {:safe => true})
    assert_equal nil, @@test.save({"_id" => nil, "foo" => "baz"}, {:safe => true})
    assert_equal nil, @@test.find_one("foo" => "bar")
    assert_equal "baz", @@test.find_one(:_id => nil)["foo"]
    assert_raise OperationFailure do
      @@test.insert({"_id" => nil, "foo" => "bar"}, {:safe => true})
    end
    assert_raise OperationFailure do
      @@test.insert({:_id => nil, "foo" => "bar"}, {:safe => true})
    end
  end

  if @@version > "1.1"
    context "distinct queries" do
      setup do
        @@test.remove
        @@test.insert([{:a => 0, :b => {:c => "a"}},
                       {:a => 1, :b => {:c => "b"}},
                       {:a => 1, :b => {:c => "c"}},
                       {:a => 2, :b => {:c => "a"}},
                       {:a => 3},
                       {:a => 3}])
      end

      should "return distinct values" do
        assert_equal [0, 1, 2, 3], @@test.distinct(:a).sort
        assert_equal ["a", "b", "c"], @@test.distinct("b.c").sort
      end

      if @@version >= "1.2"

        should "filter collection with query" do
          assert_equal [2, 3], @@test.distinct(:a, {:a => {"$gt" => 1}}).sort
        end

        should "filter nested objects" do
          assert_equal ["a", "b"], @@test.distinct("b.c", {"b.c" => {"$ne" => "c"}}).sort
        end

      end
    end
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
        @@test.update({}, {"x" => 10}, :safe => true)
      end
    end
  end

  def test_safe_save
    @@test.create_index("hello", true)

    @@test.save("hello" => "world")
    @@test.save("hello" => "world")

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

  if @@version < "1.1.1"
    def test_map_reduce
      @@test << { "user_id" => 1 }
      @@test << { "user_id" => 2 }

      m = "function() { emit(this.user_id, 1); }"
      r = "function(k,vals) { return 1; }"
      res = @@test.map_reduce(m, r);
      assert res.find_one({"_id" => 1})
      assert res.find_one({"_id" => 2})
    end

    def test_map_reduce_with_code_objects
      @@test << { "user_id" => 1 }
      @@test << { "user_id" => 2 }

      m = Code.new("function() { emit(this.user_id, 1); }")
      r = Code.new("function(k,vals) { return 1; }")
      res = @@test.map_reduce(m, r);
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
      res = @@test.map_reduce(m, r, :query => {"user_id" => {"$gt" => 1}});
      assert_equal 2, res.count
      assert res.find_one({"_id" => 2})
      assert res.find_one({"_id" => 3})
    end
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

  context "Grouping" do
    setup do 
      @@test.remove
      @@test.save("a" => 1)
      @@test.save("b" => 1)
      @initial = {"count" => 0}
      @reduce_function = "function (obj, prev) { prev.count += inc_value; }"
    end

    should "group results using eval form" do
      assert_equal 1, @@test.group([], {}, @initial, Code.new(@reduce_function, {"inc_value" => 0.5}))[0]["count"]
      assert_equal 2, @@test.group([], {}, @initial, Code.new(@reduce_function, {"inc_value" => 1}))[0]["count"]
      assert_equal 4, @@test.group([], {}, @initial, Code.new(@reduce_function, {"inc_value" => 2}))[0]["count"]
    end

    should "group results using command form" do
      assert_equal 1, @@test.group([], {}, @initial, Code.new(@reduce_function, {"inc_value" => 0.5}), true)[0]["count"]
      assert_equal 2, @@test.group([], {}, @initial, Code.new(@reduce_function, {"inc_value" => 1}), true)[0]["count"]
      assert_equal 4, @@test.group([], {}, @initial, Code.new(@reduce_function, {"inc_value" => 2}), true)[0]["count"]
    end

    should "finalize grouped results" do
      @finalize = "function(doc) {doc.f = doc.count + 200; }"
      assert_equal 202, @@test.group([], {}, @initial, Code.new(@reduce_function, {"inc_value" => 1}), true, @finalize)[0]["f"]
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
      results = @@test.group(@keyf, {}, @initial, @reduce, true).sort {|a, b| a['count'] <=> b['count']}
      assert results[0]['even'] && results[0]['count'] == 2.0
      assert results[1]['odd'] && results[1]['count'] == 3.0
    end

    should "raise an error if trying to use keyf as a group eval" do
      assert_raise OperationFailure do
        @@test.group(@keyf, {}, @initial, @reduce)
      end
    end
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

    should "remove only matching records" do
      @collection.remove({:name => "Jones"})
      assert_equal 1, @collection.size
    end
  end

  context "Creating indexes " do
    setup do
      @collection = @@db.collection('test-collection')
    end

    should "generate indexes in the proper order" do
      @collection.expects(:insert_documents) do |sel, coll, safe|
        assert_equal 'b_1_a_1', sel[:name]
      end
      @collection.create_index([['b', 1], ['a', 1]])
    end

    context "with an index created" do
      setup do
        @collection.create_index([['b', 1], ['a', 1]])
      end

      should "return properly ordered index information" do
        assert_equal [['b', 1], ['a', 1]], @collection.index_information["b_1_a_1"]
      end
    end
  end
end
