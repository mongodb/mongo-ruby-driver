$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'mongo'
require 'test/unit'

# NOTE: assumes Mongo is running
class DBTest < Test::Unit::TestCase

  include XGen::Mongo::Driver

  def setup
    @host = ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost'
    @port = ENV['MONGO_RUBY_DRIVER_PORT'] || Mongo::DEFAULT_PORT
    @db = Mongo.new(@host, @port).db('ruby-mongo-test')
  end

  def teardown
    if @db.connected?
      @db.close
    end
  end

  def test_close
    @db.close
    assert !@db.connected?
    begin
      @db.collection('test').insert('a' => 1)
      fail "expected 'NilClass' exception"
    rescue => ex
      assert_match /NilClass/, ex.to_s
    end
  end

  def test_full_coll_name
    coll = @db.collection('test')
    assert_equal 'ruby-mongo-test.test', @db.full_coll_name(coll.name)
  end

  def test_master
    # Doesn't really test anything since we probably only have one database
    # during this test.
    @db.switch_to_master
    assert @db.connected?
  end

  def test_array
    @db.close
    @db = Mongo.new([["nosuch.example.com"], [@host, @port]]).db('ruby-mongo-test')
    assert @db.connected?
  end

end
