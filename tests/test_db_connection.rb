$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'mongo'
require 'test/unit'

# NOTE: assumes Mongo is running
class DBConnectionTest < Test::Unit::TestCase

  def test_no_exceptions
    host = ENV['HOST'] || ENV['host'] || 'localhost'
    port = ENV['PORT'] || ENV['port'] || 27017
    db = XGen::Mongo::Driver::Mongo.new(host, port).db('ruby-mongo-test')
    coll = db.collection('test')
    coll.clear
  end
end
