$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'mongo'
require 'test/unit'

# NOTE: assumes Mongo is running
class AdminTest < Test::Unit::TestCase

  include XGen::Mongo::Driver

  def setup
    host = ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost'
    port = ENV['MONGO_RUBY_DRIVER_PORT'] || Mongo::DEFAULT_PORT
    @db = Mongo.new(host, port).db('ruby-mongo-test')
    # Insert some data to make sure the database itself exists.
    @coll = @db.collection('test')
    @coll.clear
    @r1 = @coll.insert('a' => 1) # collection not created until it's used
    @coll_full_name = 'ruby-mongo-test.test'
    @admin = @db.admin
  end

  def teardown
    unless @db.socket.closed?
      @admin.profiling_level = :off
      @coll.clear unless @coll == nil
    end
  end

  def test_default_profiling_level
    assert_equal :off, @admin.profiling_level
  end

  def test_change_profiling_level
    @admin.profiling_level = :slow_only
    assert_equal :slow_only, @admin.profiling_level
    @admin.profiling_level = :off
    assert_equal :off, @admin.profiling_level
  end

  def test_profiling_info
    # Perform at least one query while profiling so we have something to see.
    @admin.profiling_level = :all
    @coll.find()
    @admin.profiling_level = :off

    info = @admin.profiling_info
    assert_kind_of Array, info
    assert info.length >= 1
    first = info.first
    assert_kind_of String, first['info']
    assert_kind_of Time, first['ts']
    assert_kind_of Numeric, first['millis']
  end

  def test_validate_collection
    assert @admin.validate_collection(@coll.name)
  end

end
