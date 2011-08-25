$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require './test/replica_sets/rs_test_helper'

class ReplicaSetReconfigureTest < Test::Unit::TestCase
  include Mongo

  def setup
    @conn = ReplSetConnection.new([RS.host, RS.ports[0]])
    @db = @conn.db(MONGO_TEST_DB)
    @db.drop_collection("test-sets")
    @coll = @db.collection("test-sets")
  end

  def teardown
    RS.restart_killed_nodes
    @conn.close if @conn
  end

  def test_query
    assert @coll.save({:a => 1}, :safe => {:w => 3})
    RS.add_node
    assert_raise_error(Mongo::ConnectionFailure, "") do
      @coll.save({:a => 1}, :safe => {:w => 3})
    end
    assert @coll.save({:a => 1}, :safe => {:w => 3})
  end

  def benchmark_queries
    t1 = Time.now
    10000.times { @coll.find_one }
    Time.now - t1
  end

end
