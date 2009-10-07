$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'mongo'
require 'test/unit'
require 'logger'
require 'stringio'

# NOTE: assumes Mongo is running
class TestConnection < Test::Unit::TestCase

  include Mongo

  def setup
    @host = ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost'
    @port = ENV['MONGO_RUBY_DRIVER_PORT'] || Connection::DEFAULT_PORT
    @mongo = Connection.new(@host, @port)
  end

  def teardown
    @mongo.db('ruby-mongo-test').error
  end

  def test_invalid_database_names
    assert_raise TypeError do @mongo.db(4) end

    assert_raise InvalidName do @mongo.db('') end
    assert_raise InvalidName do @mongo.db('te$t') end
    assert_raise InvalidName do @mongo.db('te.t') end
    assert_raise InvalidName do @mongo.db('te\\t') end
    assert_raise InvalidName do @mongo.db('te/t') end
    assert_raise InvalidName do @mongo.db('te st') end
  end

  def test_database_info
    @mongo.drop_database('ruby-mongo-info-test')
    @mongo.db('ruby-mongo-info-test').collection('info-test').insert('a' => 1)

    info = @mongo.database_info
    assert_not_nil info
    assert_kind_of Hash, info
    assert_not_nil info['ruby-mongo-info-test']
    assert info['ruby-mongo-info-test'] > 0

    @mongo.drop_database('ruby-mongo-info-test')
  end

  def test_database_names
    @mongo.drop_database('ruby-mongo-info-test')
    @mongo.db('ruby-mongo-info-test').collection('info-test').insert('a' => 1)

    names = @mongo.database_names
    assert_not_nil names
    assert_kind_of Array, names
    assert names.length >= 1
    assert names.include?('ruby-mongo-info-test')

    @mongo.drop_database('ruby-mongo-info-test')
  end

  def test_logging
    output = StringIO.new
    logger = Logger.new(output)
    logger.level = Logger::DEBUG
    db = Connection.new(@host, @port, :logger => logger).db('ruby-mongo-test')
    db['test'].find().to_a

    assert output.string.include?("db.test.find")
    assert !output.string.include?("db.test.remove")
  end
  
  def test_connection_logger
    output = StringIO.new
    logger = Logger.new(output)
    logger.level = Logger::DEBUG
    connection = Connection.new(@host, @port, :logger => logger)
    assert_equal logger, connection.logger
    
    connection.logger.debug 'testing'
    assert output.string.include?('testing')
  end

  def test_drop_database
    db = @mongo.db('ruby-mongo-will-be-deleted')
    coll = db.collection('temp')
    coll.clear
    coll.insert(:name => 'temp')
    assert_equal 1, coll.count()
    assert @mongo.database_names.include?('ruby-mongo-will-be-deleted')

    @mongo.drop_database('ruby-mongo-will-be-deleted')
    assert !@mongo.database_names.include?('ruby-mongo-will-be-deleted')
  end

  def test_pair
    db = Connection.new({:left => ['foo', 123]})
    pair = db.instance_variable_get('@pair')
    assert_equal 2, pair.length
    assert_equal ['foo', 123], pair[0]
    assert_equal ['localhost', Connection::DEFAULT_PORT], pair[1]

    db = Connection.new({:right => 'bar'})
    pair = db.instance_variable_get('@pair')
    assert_equal 2, pair.length
    assert_equal ['localhost', Connection::DEFAULT_PORT], pair[0]
    assert_equal ['bar', Connection::DEFAULT_PORT], pair[1]

    db = Connection.new({:right => ['foo', 123], :left => 'bar'})
    pair = db.instance_variable_get('@pair')
    assert_equal 2, pair.length
    assert_equal ['bar', Connection::DEFAULT_PORT], pair[0]
    assert_equal ['foo', 123], pair[1]
  end
end
