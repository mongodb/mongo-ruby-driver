$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'mongo'
require 'test/unit'

# NOTE: assumes Mongo is running
class DBConnectionTest < Test::Unit::TestCase

  def test_no_exceptions
    db = XGen::Mongo::Driver::Mongo.new.db('ruby-mongo-demo')
    coll = db.collection('test')
    coll.clear
  end
end
