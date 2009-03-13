$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'mongo'
require 'test/unit'

# NOTE: assumes Mongo is running
class CursorTest < Test::Unit::TestCase

  include XGen::Mongo::Driver

  @@db = Mongo.new(ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost',
                   ENV['MONGO_RUBY_DRIVER_PORT'] || Mongo::DEFAULT_PORT).db('ruby-mongo-test')
  @@coll = @@db.collection('test')

  def setup
    @@coll.clear
    @@coll.insert('a' => 1)     # collection not created until it's used
    @@coll_full_name = 'ruby-mongo-test.test'
  end

  def teardown
    @@coll.clear
    @@db.error
  end

  def test_explain
    cursor = @@coll.find('a' => 1)
    explaination = cursor.explain
    assert_not_nil explaination['cursor']
    assert_kind_of Numeric, explaination['n']
    assert_kind_of Numeric, explaination['millis']
    assert_kind_of Numeric, explaination['nscanned']
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
    begin
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
    rescue Test::Unit::AssertionFailedError => ex
      p @@db.collection_names
      Process.exit 1
    end
  end

  def test_refill_via_get_more_alt_coll
    begin
      coll = @@db.collection('test-alt-coll')
      coll.clear
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
    rescue Test::Unit::AssertionFailedError => ex
      p @@db.collection_names
      Process.exit 1
    end
  end

  def test_close_after_query_sent
    begin
      cursor = @@coll.find('a' => 1)
      cursor.next_object
      cursor.close
      assert cursor.closed?
    rescue => ex
      fail ex.to_s
    end
  end

end
