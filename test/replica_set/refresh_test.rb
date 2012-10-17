require 'test_helper'
require 'benchmark'

class ReplicaSetRefreshTest < Test::Unit::TestCase

  def setup
    ensure_cluster(:rs)
  end

  def self.shutdown
    @@cluster.stop
    @@cluster.clobber
  end

  def test_connect_and_manual_refresh_with_secondaries_down
    @rs.secondaries.each{|s| s.stop}

    rescue_connection_failure do
      @conn = ReplSetConnection.new(@rs.repl_set_seeds, :refresh_mode => false)
    end

    assert_equal [], @conn.secondaries
    assert @conn.connected?
    assert_equal @conn.read_pool, @conn.primary_pool

    # Refresh with no change to set
    @conn.refresh
    assert_equal [], @conn.secondaries
    assert @conn.connected?
    assert_equal @conn.read_pool, @conn.primary_pool

    # Test no changes after restart until manual refresh
    @rs.restart
    assert_equal [], @conn.secondaries
    assert @conn.connected?
    assert_equal @conn.read_pool, @conn.primary_pool

    # Refresh and ensure state
    @conn.refresh
    assert_equal @conn.read_pool, @conn.primary_pool
    assert_equal 2, @conn.secondaries.length
  end

  def test_automated_refresh_with_secondaries_down
    @rs.secondaries.each{|s| s.stop}
    
    rescue_connection_failure do
      @conn = ReplSetConnection.new(@rs.repl_set_seeds,
        :refresh_interval => 1, :refresh_mode => :sync, :read => :secondary_preferred)
    end

    # Ensure secondaries not available and read from primary
    assert_equal [], @conn.secondaries
    assert @conn.connected?
    assert_equal @conn.manager.read, @conn.manager.primary
    old_refresh_version = @conn.refresh_version

    # Restart nodes and ensure refresh interval has passed
    @rs.restart
    sleep(2)

    assert @conn.refresh_version == old_refresh_version,
      "Refresh version has changed."

    # Trigger synchronous refresh
    @conn['foo']['bar'].find_one

    assert @conn.refresh_version > old_refresh_version,
      "Refresh version hasn't changed."
    assert @conn.secondaries.length == 2,
      "No secondaries have been added."
    assert @conn.manager.read != @conn.manager.primary,
      "Read pool and primary pool are identical."
  end
  
  def test_automated_refresh_when_secondary_goes_down
    @conn = ReplSetConnection.new(@rs.repl_set_seeds,
      :refresh_interval => 1, :refresh_mode => :sync)

    num_secondaries = @conn.secondary_pools.length
    old_refresh_version = @conn.refresh_version

    @rs.secondaries.first.kill
    sleep(2)

    assert @conn.refresh_version == old_refresh_version,
      "Refresh version has changed."

    @conn['foo']['bar'].find_one

    assert @conn.refresh_version > old_refresh_version,
      "Refresh version hasn't changed."
    assert_equal num_secondaries - 1, @conn.secondaries.length
    assert_equal num_secondaries - 1, @conn.secondary_pools.length

    @rs.start
    sleep(2)

    @conn['foo']['bar'].find_one

    assert_equal num_secondaries, @conn.secondaries.length
    assert_equal num_secondaries, @conn.secondary_pools.length
  end
=begin
  def test_automated_refresh_with_removed_node
    @conn = ReplSetConnection.new(@rs.repl_set_seeds,
      :refresh_interval => 1, :refresh_mode => :sync)

    num_secondaries = @conn.secondary_pools.length
    old_refresh_version = @conn.refresh_version

    n = @rs.repl_set_remove_node(2)
    sleep(2)

    rescue_connection_failure do
      @conn['foo']['bar'].find_one
    end

    assert @conn.refresh_version > old_refresh_version,
      "Refresh version hasn't changed."
    assert_equal num_secondaries - 1, @conn.secondaries.length
    assert_equal num_secondaries - 1, @conn.secondary_pools.length

    #@rs.add_node(n)
  end

  def test_adding_and_removing_nodes
    @conn = ReplSetConnection.new(build_seeds(3),
      :refresh_interval => 2, :refresh_mode => :sync)

    @rs.add_node
    sleep(4)
    @conn['foo']['bar'].find_one

    @conn2 = ReplSetConnection.new(build_seeds(3),
      :refresh_interval => 2, :refresh_mode => :sync)

    assert @conn2.secondaries.sort == @conn.secondaries.sort,
      "Second connection secondaries not equal to first."
    assert_equal 3, @conn.secondary_pools.length
    assert_equal 3, @conn.secondaries.length

    config = @conn['admin'].command({:ismaster => 1})

    @rs.remove_secondary_node
    sleep(4)
    config = @conn['admin'].command({:ismaster => 1})

    assert_equal 2, @conn.secondary_pools.length
    assert_equal 2, @conn.secondaries.length
  end
=end
end
