require 'test/test_helper'

# NOTE: assumes Mongo is running
class DBConnectionTest < Test::Unit::TestCase

  include Mongo

  def test_no_exceptions
    host = ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost'
    port = ENV['MONGO_RUBY_DRIVER_PORT'] || Connection::DEFAULT_PORT
    db = Connection.new(host, port).db('ruby-mongo-demo')
    coll = db.collection('test')
    coll.remove
    db.error
  end
end
