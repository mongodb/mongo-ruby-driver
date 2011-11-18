$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require './test/replica_sets/rs_test_helper'
require 'benchmark'

class ReplicaSetRefreshTest < Test::Unit::TestCase
  include ReplicaSetTest

  def setup
    @conn = nil
  end

  def teardown
    self.rs.restart_killed_nodes
    @conn.close if @conn
  end

  def test_connect_speed
    Benchmark.bm do |x|
      x.report("Connect") do
        10.times do
          ReplSetConnection.new([self.rs.host, self.rs.ports[0]],
                                [self.rs.host, self.rs.ports[1]],
                                [self.rs.host, self.rs.ports[2]],
                                :refresh_mode => false)
        end
      end

          @con = ReplSetConnection.new([self.rs.host, self.rs.ports[0]],
                                       [self.rs.host, self.rs.ports[1]],
                                       [self.rs.host, self.rs.ports[2]],
                                       :refresh_mode => false)

      x.report("manager") do
        man = Mongo::PoolManager.new(@con, @con.seeds)
        10.times do
          man.connect
        end
      end
    end
  end

  def test_connect_and_manual_refresh_with_secondaries_down
    self.rs.kill_all_secondaries

    rescue_connection_failure do
      @conn = ReplSetConnection.new([self.rs.host, self.rs.ports[0]],
                                    [self.rs.host, self.rs.ports[1]],
                                    [self.rs.host, self.rs.ports[2]],
                                    :refresh_mode => false)
    end

    assert_equal [], @conn.secondaries
    assert @conn.connected?
    assert_equal @conn.read_pool, @conn.primary_pool

    # Refresh with no change to set
    @conn.refresh
    assert_equal [], @conn.secondaries
    assert @conn.connected?
    assert_equal @conn.read_pool, @conn.primary_pool

    self.rs.restart_killed_nodes
    assert_equal [], @conn.secondaries
    assert @conn.connected?
    assert_equal @conn.read_pool, @conn.primary_pool

    # Refresh with everything up
    @conn.refresh
    assert @conn.read_pool
    assert @conn.secondaries.length > 0
  end

  def test_automated_refresh_with_secondaries_down
    self.rs.kill_all_secondaries

    rescue_connection_failure do
      @conn = ReplSetConnection.new([self.rs.host, self.rs.ports[0]],
                                    [self.rs.host, self.rs.ports[1]],
                                    [self.rs.host, self.rs.ports[2]],
                                    :refresh_interval => 2,
                                    :refresh_mode => :sync)
    end

    assert_equal [], @conn.secondaries
    assert @conn.connected?
    assert_equal @conn.read_pool, @conn.primary_pool
    old_refresh_version = @conn.refresh_version

    self.rs.restart_killed_nodes
    sleep(4)
    @conn['foo']['bar'].find_one
    @conn['foo']['bar'].insert({:a => 1})

    assert @conn.refresh_version > old_refresh_version,
      "Refresh version hasn't changed."
    assert @conn.secondaries.length > 0,
      "No secondaries have been added."
    assert @conn.read_pool != @conn.primary_pool,
      "Read pool and primary pool are identical."
  end

  def test_automated_refresh_with_removed_node
    @conn = ReplSetConnection.new([self.rs.host, self.rs.ports[0]],
                                  [self.rs.host, self.rs.ports[1]],
                                  [self.rs.host, self.rs.ports[2]],
                                  :refresh_interval => 2,
                                  :refresh_mode => :sync)

    num_secondaries = @conn.secondary_pools.length
    old_refresh_version = @conn.refresh_version

    n = self.rs.remove_secondary_node
    sleep(4)
    @conn['foo']['bar'].find_one

    assert @conn.refresh_version > old_refresh_version,
      "Refresh version hasn't changed."
    assert_equal num_secondaries - 1, @conn.secondaries.length
    assert_equal num_secondaries - 1, @conn.secondary_pools.length

    self.rs.add_node(n)
  end

  def test_adding_and_removing_nodes
    @conn = ReplSetConnection.new([self.rs.host, self.rs.ports[0]],
                                  [self.rs.host, self.rs.ports[1]],
                                  [self.rs.host, self.rs.ports[2]],
                                  :refresh_interval => 2, :refresh_mode => :sync)

    self.rs.add_node
    sleep(4)
    @conn['foo']['bar'].find_one

    @conn2 = ReplSetConnection.new([self.rs.host, self.rs.ports[0]],
                                   [self.rs.host, self.rs.ports[1]],
                                   [self.rs.host, self.rs.ports[2]],
                                   :refresh_interval => 2, :refresh_mode => :sync)

    assert @conn2.secondaries.sort == @conn.secondaries.sort,
      "Second connection secondaries not equal to first."
    assert_equal 3, @conn.secondary_pools.length
    assert_equal 3, @conn.secondaries.length

    config = @conn['admin'].command({:ismaster => 1})

    self.rs.remove_secondary_node
    sleep(4)
    config = @conn['admin'].command({:ismaster => 1})

    assert_equal 2, @conn.secondary_pools.length
    assert_equal 2, @conn.secondaries.length
  end
end
