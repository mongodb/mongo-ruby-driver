require 'test_helper'

class DBConnectionTest < Test::Unit::TestCase

  def test_no_exceptions
    host = ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost'
    port = ENV['MONGO_RUBY_DRIVER_PORT'] || MongoClient::DEFAULT_PORT
    db = MongoClient.new(host, port).db(MONGO_TEST_DB)
    coll = db.collection('test')
    coll.remove
    db.get_last_error
  end
end
