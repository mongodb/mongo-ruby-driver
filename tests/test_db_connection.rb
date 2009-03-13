$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'mongo'
require 'test/unit'

# NOTE: assumes Mongo is running
class DBConnectionTest < Test::Unit::TestCase

  include XGen::Mongo::Driver

  def test_no_exceptions
    host = ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost'
    port = ENV['MONGO_RUBY_DRIVER_PORT'] || Mongo::DEFAULT_PORT
    db = Mongo.new(host, port).db('ruby-mongo-demo')
    coll = db.collection('test')
    coll.clear
    db.error
  end
end
