$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require './test/replica_sets/rs_test_helper'

class ReplicaSetCountTest < Test::Unit::TestCase
  include ReplicaSetTest

  def setup
    @conn = ReplSetConnection.new([self.rs.host, self.rs.ports[0]],
                                  [self.rs.host, self.rs.ports[1]], [self.rs.host, self.rs.ports[2]],
                                  :read => :secondary)
    assert @conn.primary_pool
    @primary = Connection.new(@conn.primary_pool.host, @conn.primary_pool.port)
    @db = @conn.db(MONGO_TEST_DB)
    @db.drop_collection("test-sets")
    @coll = @db.collection("test-sets")
  end

  def teardown
    self.rs.restart_killed_nodes
    @conn.close if @conn
  end

  def test_correct_count_after_insertion_reconnect
    @coll.insert({:a => 20}, :safe => {:w => 2, :wtimeout => 10000})
    assert_equal 1, @coll.count

    # Kill the current master node
    @node = self.rs.kill_primary

    rescue_connection_failure do
      @coll.insert({:a => 30}, :safe => true)
    end

    @coll.insert({:a => 40}, :safe => true)
    assert_equal 3, @coll.count, "Second count failed"
  end

  def test_count_command_sent_to_primary
    @coll.insert({:a => 20}, :safe => {:w => 2, :wtimeout => 10000})
    count_before = @primary['admin'].command({:serverStatus => 1})['opcounters']['command']
    assert_equal 1, @coll.count
    count_after = @primary['admin'].command({:serverStatus => 1})['opcounters']['command']
    assert_equal 2, count_after - count_before
  end
end
