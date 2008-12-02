$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'mongo'
require 'test/unit'

# NOTE: assumes Mongo is running
class DBAPITest < Test::Unit::TestCase

  def setup
    @db = XGen::Mongo::Driver::Mongo.new.db('ruby-mongo-test')
    @coll = @db.collection('test')
    @coll.clear
  end

  def teardown
    @coll.clear
  end

  def test_clear
    @coll.insert('a' => 1)
    assert_equal 1, @coll.count
    @coll.clear
    assert_equal 0, @coll.count
  end
end
