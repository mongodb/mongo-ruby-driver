$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require './test/replica_sets/rs_test_helper'
require 'benchmark'

# on ports TEST_PORT, RS.ports[1], and TEST + 2.
class ReplicaSetRefreshTest < Test::Unit::TestCase
  include Mongo

  def setup
    @conn = nil
  end

  def teardown
    RS.restart_killed_nodes
    @conn.close if @conn
  end

  def test_connect_speed
    Benchmark.bm do |x|
      x.report("Connect") do
        10.times do
          ReplSetConnection.new([RS.host, RS.ports[0]], [RS.host, RS.ports[1]],
            [RS.host, RS.ports[2]], :background_refresh => false)
        end
      end

          @con = ReplSetConnection.new([RS.host, RS.ports[0]], [RS.host, RS.ports[1]],
            [RS.host, RS.ports[2]], :background_refresh => false)

      x.report("manager") do
        man = Mongo::PoolManager.new(@con, @con.seeds)
        10.times do
          man.connect
        end
      end
    end
  end

  def test_connect_and_manual_refresh_with_secondaries_down
    RS.kill_all_secondaries

    rescue_connection_failure do
      @conn = ReplSetConnection.new([RS.host, RS.ports[0]], [RS.host, RS.ports[1]],
        [RS.host, RS.ports[2]], :background_refresh => false)
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
        [RS.host, RS.ports[2]], :refresh_interval => 2, :background_refresh => true)
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
      [RS.host, RS.ports[2]], :refresh_interval => 2, :background_refresh => true)

    assert_equal 2, @conn.secondary_pools.length
    assert_equal 2, @conn.secondaries.length

    n = RS.remove_secondary_node
    sleep(4)

    assert_equal 1, @conn.secondaries.length
    assert_equal 1, @conn.secondary_pools.length

    RS.add_node(n)
  end

  def test_adding_and_removing_nodes
    @conn = ReplSetConnection.new([RS.host, RS.ports[0]], [RS.host, RS.ports[1]],
      [RS.host, RS.ports[2]], :refresh_interval => 2, :background_refresh => true)

    RS.add_node
    sleep(5)

    @conn2 = ReplSetConnection.new([RS.host, RS.ports[0]], [RS.host, RS.ports[1]],
      [RS.host, RS.ports[2]], :refresh_interval => 2, :background_refresh => true)

    assert @conn2.secondaries == @conn.secondaries
    assert_equal 3, @conn.secondary_pools.length
    assert_equal 3, @conn.secondaries.length

    RS.remove_secondary_node
    sleep(4)
    assert_equal 2, @conn.secondary_pools.length
    assert_equal 2, @conn.secondaries.length
  end
end
