$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'mongo'
require 'test/unit'

# NOTE: assumes Mongo is running
class DBAPITest < Test::Unit::TestCase

  def setup
    @db = XGen::Mongo::Driver::Mongo.new.db('ruby-mongo-test')
    @coll = @db.collection('test')
    @coll.clear
    @coll.insert('a' => 1)      # collection not created until it's used
    @coll_full_name = 'ruby-mongo-test.test'
  end

  def teardown
    @coll.clear unless @db.socket.closed?
  end

  def test_clear
    assert_equal 1, @coll.count
    @coll.clear
    assert_equal 0, @coll.count
  end

  def test_insert
    @coll.insert('a' => 2)
    @coll.insert('b' => 3)

    assert_equal 3, @coll.count
    docs = @coll.find().collect
    assert_equal 3, docs.length
    assert docs.include?('a' => 1)
    assert docs.include?('a' => 2)
    assert docs.include?('b' => 3)
  end

  def test_close
    @db.close
    assert @db.socket.closed?
    begin
      @coll.insert('a' => 1)
      fail "expected IOError exception"
    rescue IOError => ex
      assert_match /closed stream/, ex.to_s
    end
  end

  def test_drop_collection
    assert @db.drop_collection(@coll.name), "drop of collection #{@coll.name} failed"
    assert_equal 0, @db.collection_names.length
  end

  def test_collection_names
    names = @db.collection_names
    assert_equal 1, names.length
    assert_equal 'ruby-mongo-test.test', names[0]

    coll2 = @db.collection('test2')
    coll2.insert('a' => 1)      # collection not created until it's used
    names = @db.collection_names
    assert_equal 2, names.length
    assert names.include?('ruby-mongo-test.test')
    assert names.include?('ruby-mongo-test.test2')
  ensure
    @db.drop_collection('test2')
  end

  def test_collections_info
    cursor = @db.collections_info
    rows = cursor.collect
    assert_equal 1, rows.length
    row = rows[0]
    assert_equal @coll_full_name, row['name']
# FIXME restore this test when Mongo fixes this bug (or we prove I'm doing something wrong)
    # Mongo bug: returns string with wrong length, so last byte of value is chopped off.
#     assert_equal @coll.name, row['options']['create']
  end
end
