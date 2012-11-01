$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require './test/replica_sets/rs_test_helper'
require 'benchmark'

class ReplicaSetRefreshTest < Test::Unit::TestCase

  def setup
    ensure_rs
  end

  def teardown
    @rs.restart_killed_nodes
    @client.close if defined?(@conn)
  end

=begin
  def test_connect_speed
    Benchmark.bm do |x|
      x.report("Connect") do
        10.times do
          ReplSetClient.new(build_seeds(3), :refresh_mode => false)
        end
      end

      @con = ReplSetClient.new(build_seeds(3), :refresh_mode => false)

      x.report("manager") do
        man = Mongo::PoolManager.new(@con, @con.seeds)
        10.times do
          man.connect
        end
      end
    end
  end
=end

  def test_connect_and_manual_refresh_with_secondaries_down
    @rs.kill_all_secondaries
    sleep(4)

    rescue_connection_failure do
      @client = ReplSetClient.new(build_seeds(3), :refresh_mode => false)
    end

    assert_equal [], @client.secondaries
    assert @client.connected?
    assert_equal @client.read_pool, @client.primary_pool

    # Refresh with no change to set
    @client.refresh
    assert_equal [], @client.secondaries
    assert @client.connected?
    assert_equal @client.read_pool, @client.primary_pool

    @rs.restart_killed_nodes
    assert_equal [], @client.secondaries
    assert @client.connected?
    assert_equal @client.read_pool, @client.primary_pool

    # Refresh with everything up
    @client.refresh
    assert @client.read_pool
    assert @client.secondaries.length > 0
  end

  def test_automated_refresh_with_secondaries_down
    @rs.kill_all_secondaries
    sleep(4)
    
    rescue_connection_failure do
      @client = ReplSetClient.new(build_seeds(3),
        :refresh_interval => 2, :refresh_mode => :sync, :read => :secondary_preferred)
    end

    assert_equal [], @client.secondaries
    assert @client.connected?
    assert_equal @client.manager.read, @client.manager.primary
    old_refresh_version = @client.refresh_version

    @rs.restart_killed_nodes
    sleep(4)
    @client['foo']['bar'].find_one
    @client['foo']['bar'].insert({:a => 1})

    assert @client.refresh_version > old_refresh_version,
      "Refresh version hasn't changed."
    assert @client.secondaries.length > 0,
      "No secondaries have been added."
    assert @client.manager.read != @client.manager.primary,
      "Read pool and primary pool are identical."
  end
  
  def test_automated_refresh_when_secondary_goes_down
    @client = ReplSetClient.new(build_seeds(3),
      :refresh_interval => 2, :refresh_mode => :sync)

    num_secondaries = @client.secondary_pools.length
    old_refresh_version = @client.refresh_version

    @rs.kill_secondary
    sleep(4)
    @client['foo']['bar'].find_one

    assert @client.refresh_version > old_refresh_version,
      "Refresh version hasn't changed."
    assert_equal num_secondaries - 1, @client.secondaries.length
    assert_equal num_secondaries - 1, @client.secondary_pools.length

    @rs.restart_killed_nodes
  end

  def test_automated_refresh_with_removed_node
    @client = ReplSetClient.new(build_seeds(3),
      :refresh_interval => 2, :refresh_mode => :sync)

    num_secondaries = @client.secondary_pools.length
    old_refresh_version = @client.refresh_version

    n = @rs.remove_secondary_node
    sleep(4)
    @client['foo']['bar'].find_one

    assert @client.refresh_version > old_refresh_version,
      "Refresh version hasn't changed."
    assert_equal num_secondaries - 1, @client.secondaries.length
    assert_equal num_secondaries - 1, @client.secondary_pools.length

    @rs.add_node(n)
  end

  def test_adding_and_removing_nodes
    @client = ReplSetClient.new(build_seeds(3),
      :refresh_interval => 2, :refresh_mode => :sync)

    @rs.add_node
    sleep(4)
    @client['foo']['bar'].find_one

    @conn2 = ReplSetClient.new(build_seeds(3),
      :refresh_interval => 2, :refresh_mode => :sync)

    assert @conn2.secondaries.sort == @client.secondaries.sort,
      "Second connection secondaries not equal to first."
    assert_equal 3, @client.secondary_pools.length
    assert_equal 3, @client.secondaries.length

    config = @client['admin'].command({:ismaster => 1})

    @rs.remove_secondary_node
    sleep(4)
    config = @client['admin'].command({:ismaster => 1})

    assert_equal 2, @client.secondary_pools.length
    assert_equal 2, @client.secondaries.length
  end
end
