require 'test/test_helper'
require 'logger'

# NOTE: assumes Mongo is running
class CursorTest < Test::Unit::TestCase

  include Mongo

  @@connection = Connection.new(ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost',
                        ENV['MONGO_RUBY_DRIVER_PORT'] || Connection::DEFAULT_PORT)
  @@db   = @@connection.db('ruby-mongo-test')
  @@coll = @@db.collection('test')
  @@version = @@connection.server_version

  def setup
    @@coll.remove
    @@coll.insert('a' => 1)     # collection not created until it's used
    @@coll_full_name = 'ruby-mongo-test.test'
  end

  def test_explain
    cursor = @@coll.find('a' => 1)
    explaination = cursor.explain
    assert_not_nil explaination['cursor']
    assert_kind_of Numeric, explaination['n']
    assert_kind_of Numeric, explaination['millis']
    assert_kind_of Numeric, explaination['nscanned']
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

  def test_next_object_deprecation
    @@coll.remove
    @@coll.insert({"a" => 1})

    assert_equal 1, @@coll.find().next_object["a"]
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
    assert_equal -1000000,   results[1]['n']
    assert_equal 1000000,    results[2]['n']
    assert_equal MaxKey.new, results[3]['n']
  end

  def test_id_range_queries
    @@coll.remove

    t1 = Time.now
    t1_id = ObjectID.from_time(t1)
    @@coll.save({:t => 't1'})
    @@coll.save({:t => 't1'})
    @@coll.save({:t => 't1'})
    sleep(2)
    t2 = Time.now
    t2_id = ObjectID.from_time(t2)
    @@coll.save({:t => 't2'})
    @@coll.save({:t => 't2'})
    @@coll.save({:t => 't2'})

    assert_equal 3, @@coll.find({'_id' => {'$gt' => t1_id}, '_id' => {'$lt' => t2_id}}).count
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

  def test_limit_exceptions
    assert_raise ArgumentError do
      cursor = @@coll.find().limit('not-an-integer')
    end

    cursor      = @@coll.find()
    firstResult = cursor.next_document
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
    assert_raise ArgumentError do
      cursor = @@coll.find().skip('not-an-integer')
    end

    cursor      = @@coll.find()
    firstResult = cursor.next_document
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
    by_location = @@db.command("cursorInfo" => 1)["byLocation_size"]

    10000.times do |i|
      @@coll.insert("i" => i)
    end

    assert_equal(client_cursors,
                 @@db.command("cursorInfo" => 1)["clientCursors_size"])
    assert_equal(by_location,
                 @@db.command("cursorInfo" => 1)["byLocation_size"])

    10.times do |i|
      @@coll.find_one()
    end

    assert_equal(client_cursors,
                 @@db.command("cursorInfo" => 1)["clientCursors_size"])
    assert_equal(by_location,
                 @@db.command("cursorInfo" => 1)["byLocation_size"])

    10.times do |i|
      a = @@coll.find()
      a.next_document
      a.close()
    end

    assert_equal(client_cursors,
                 @@db.command("cursorInfo" => 1)["clientCursors_size"])
    assert_equal(by_location,
                 @@db.command("cursorInfo" => 1)["byLocation_size"])

    a = @@coll.find()
    a.next_document

    assert_not_equal(client_cursors,
                     @@db.command("cursorInfo" => 1)["clientCursors_size"])
    assert_not_equal(by_location,
                     @@db.command("cursorInfo" => 1)["byLocation_size"])

    a.close()

    assert_equal(client_cursors,
                 @@db.command("cursorInfo" => 1)["clientCursors_size"])
    assert_equal(by_location,
                 @@db.command("cursorInfo" => 1)["byLocation_size"])

    a = @@coll.find({}, :limit => 10).next_document

    assert_equal(client_cursors,
                 @@db.command("cursorInfo" => 1)["clientCursors_size"])
    assert_equal(by_location,
                 @@db.command("cursorInfo" => 1)["byLocation_size"])

    @@coll.find() do |cursor|
      cursor.next_document
    end

    assert_equal(client_cursors,
                 @@db.command("cursorInfo" => 1)["clientCursors_size"])
    assert_equal(by_location,
                 @@db.command("cursorInfo" => 1)["byLocation_size"])

    @@coll.find() { |cursor|
      cursor.next_document
    }

    assert_equal(client_cursors,
                 @@db.command("cursorInfo" => 1)["clientCursors_size"])
    assert_equal(by_location,
                 @@db.command("cursorInfo" => 1)["byLocation_size"])
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
end
