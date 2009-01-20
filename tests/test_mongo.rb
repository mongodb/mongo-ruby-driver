$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'mongo'
require 'test/unit'

# NOTE: assumes Mongo is running
class MongoTest < Test::Unit::TestCase

  include XGen::Mongo::Driver

  def setup
    @host = ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost'
    @port = ENV['MONGO_RUBY_DRIVER_PORT'] || Mongo::DEFAULT_PORT
    @mongo = Mongo.new(@host, @port)
  end

  def test_database_info
    info = @mongo.database_info
    assert_not_nil info
    assert_kind_of Hash, info
    assert_not_nil info['admin']
    assert info['admin'] > 0
  end

  def test_database_names
    names = @mongo.database_names
    assert_not_nil names
    assert_kind_of Array, names
    assert names.length >= 1
    assert names.include?('admin')
  end

  def test_drop_database
    db = @mongo.db('will-be-deleted')
    coll = db.collection('temp')
    coll.clear
    coll.insert(:name => 'temp')
    assert_equal 1, coll.count()
    assert @mongo.database_names.include?('will-be-deleted')

    @mongo.drop_database('will-be-deleted')
    assert !@mongo.database_names.include?('will-be-deleted')
  end

end
