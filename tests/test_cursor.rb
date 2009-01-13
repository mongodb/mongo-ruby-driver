$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'mongo'
require 'test/unit'

# NOTE: assumes Mongo is running
class CursorTest < Test::Unit::TestCase

  include XGen::Mongo::Driver

  def setup
    host = ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost'
    port = ENV['MONGO_RUBY_DRIVER_PORT'] || Mongo::DEFAULT_PORT
    @db = Mongo.new(host, port).db('ruby-mongo-test')
    @coll = @db.collection('test')
    @coll.clear
    @r1 = @coll.insert('a' => 1) # collection not created until it's used
    @coll_full_name = 'ruby-mongo-test.test'
  end

  def teardown
    @coll.clear unless @coll == nil || @db.socket.closed?
  end

  def test_explain
    cursor = @coll.find('a' => 1)
    explaination = cursor.explain
    assert_not_nil explaination['cursor']
    assert_kind_of Numeric, explaination['n']
    assert_kind_of Numeric, explaination['millis']
    assert_kind_of Numeric, explaination['nscanned']
  end

  def test_close_no_query_sent
    begin
      cursor = @coll.find('a' => 1)
      cursor.close
      assert cursor.closed?
    rescue => ex
      fail ex.to_s
    end
  end

  def test_close_after_query_sent
    begin
      cursor = @coll.find('a' => 1)
      cursor.next_object
      cursor.close
      assert cursor.closed?
    rescue => ex
      fail ex.to_s
    end
  end

  def test_hint
    begin
      cursor = @coll.find('a' => 1).hint('a')
      assert_equal 1, cursor.to_a.size
    rescue => ex
      fail ex.to_s
    end
  end

end
