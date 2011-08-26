$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require './test/replica_sets/rs_test_helper'
require 'benchmark'

# NOTE: This test expects a replica set of three nodes to be running on RS.host,
# on ports TEST_PORT, RS.ports[1], and TEST + 2.
class ReplicaSetRefreshTest < Test::Unit::TestCase
  include Mongo

  def teardown
    RS.restart_killed_nodes
    @conn.close if @conn
  end

  def test_connect_and_manual_refresh_with_secondaries_down
    RS.kill_all_secondaries

    rescue_connection_failure do
      @conn = ReplSetConnection.new([RS.host, RS.ports[0]], [RS.host, RS.ports[1]],
        [RS.host, RS.ports[2]], :auto_refresh => false)
    end

    assert_equal [], @conn.secondaries
    assert @conn.connected?
    assert_equal @conn.read_pool, @conn.primary_pool

    # Refresh with no change to set
    @conn.refresh
    assert_equal [], @conn.secondaries
    assert @conn.connected?
    assert_equal @conn.read_pool, @conn.primary_pool

    RS.restart_killed_nodes
    assert_equal [], @conn.secondaries
    assert @conn.connected?
    assert_equal @conn.read_pool, @conn.primary_pool

    # Refresh with everything up
    @conn.refresh
    assert @conn.read_pool
    assert @conn.secondaries.length > 0
  end

  def test_automated_refresh_with_secondaries_down
    RS.kill_all_secondaries

    rescue_connection_failure do
      @conn = ReplSetConnection.new([RS.host, RS.ports[0]], [RS.host, RS.ports[1]],
        [RS.host, RS.ports[2]], :refresh_interval => 2, :auto_refresh => true)
    end

    assert_equal [], @conn.secondaries
    assert @conn.connected?
    assert_equal @conn.read_pool, @conn.primary_pool

    RS.restart_killed_nodes

    sleep(3)

    assert @conn.read_pool != @conn.primary_pool, "Read pool and primary pool are identical."
    assert @conn.secondaries.length > 0, "No secondaries have been added."
  end

  def test_automated_refresh_with_removed_node
    @conn = ReplSetConnection.new([RS.host, RS.ports[0]], [RS.host, RS.ports[1]],
      [RS.host, RS.ports[2]], :refresh_interval => 2, :auto_refresh => true)

    assert_equal 2, @conn.secondaries.length
    assert_equal 2, @conn.secondary_pools.length

    RS.remove_secondary_node
    sleep(3)

    assert_equal 1, @conn.secondaries.length
    assert_equal 1, @conn.secondary_pools.length
  end

end
