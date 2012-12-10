require 'test_helper'
require 'benchmark'

class ReplicaSetRefreshTest < Test::Unit::TestCase

  def setup
    ensure_cluster(:rs)
  end

  def test_connect_and_manual_refresh_with_secondaries_down
    @rs.secondaries.each{|s| s.stop}
    client = MongoReplicaSetClient.new(@rs.repl_set_seeds, :refresh_mode => false)

    assert_equal Set.new, client.secondaries
    assert client.connected?
    assert_equal client.read_pool, client.primary_pool

    # Refresh with no change to set
    client.refresh
    assert_equal Set.new, client.secondaries
    assert client.connected?
    assert_equal client.read_pool, client.primary_pool

    # Test no changes after restart until manual refresh
    @rs.restart
    assert_equal Set.new, client.secondaries
    assert client.connected?
    assert_equal client.read_pool, client.primary_pool

    # Refresh and ensure state
    client.refresh
    assert_equal client.read_pool, client.primary_pool
    assert_equal 1, client.secondaries.length
  end

  def test_automated_refresh_with_secondaries_down
    @rs.secondaries.each{|s| s.stop}
    client = MongoReplicaSetClient.new(@rs.repl_set_seeds,
      :refresh_interval => 1, :refresh_mode => :sync, :read => :secondary_preferred)

    # Ensure secondaries not available and read from primary
    assert_equal Set.new, client.secondaries
    assert client.connected?
    assert client.manager.pools.member?(client.manager.read_pool)
    old_refresh_version = client.refresh_version

    # Restart nodes and ensure refresh interval has passed
    @rs.restart
    sleep(2)

    assert client.refresh_version == old_refresh_version,
      "Refresh version has changed."

    # Trigger synchronous refresh
    client['foo']['bar'].find_one

    assert client.refresh_version > old_refresh_version,
      "Refresh version hasn't changed."
    assert client.secondaries.length == 1,
      "No secondaries have been added."
    assert client.manager.read_pool != client.manager.primary,
      "Read pool and primary pool are identical."
  end
  
  def test_automated_refresh_when_secondary_goes_down
    client = MongoReplicaSetClient.new(@rs.repl_set_seeds,
      :refresh_interval => 1, :refresh_mode => :sync)

    num_secondaries = client.secondary_pools.length
    old_refresh_version = client.refresh_version

    @rs.kill_secondary
    sleep(1)

    assert client.refresh_version == old_refresh_version,
      "Refresh version has changed."

    client['foo']['bar'].find_one

    assert client.refresh_version > old_refresh_version,
      "Refresh version hasn't changed."
    assert_equal num_secondaries - 1, client.secondaries.length
    assert_equal num_secondaries - 1, client.secondary_pools.length

    @rs.start
    sleep(2)

    client['foo']['bar'].find_one

    assert_equal num_secondaries, client.secondaries.length
    assert_equal num_secondaries, client.secondary_pools.length
  end
=begin
  def test_automated_refresh_with_removed_node
    client = MongoReplicaSetClient.new(@rs.repl_set_seeds,
      :refresh_interval => 1, :refresh_mode => :sync)

    num_secondaries = client.secondary_pools.length
    old_refresh_version = client.refresh_version

    n = @rs.repl_set_remove_node(2)
    sleep(2)

    rescue_connection_failure do
      client['foo']['bar'].find_one
    end

    assert client.refresh_version > old_refresh_version,
      "Refresh version hasn't changed."
    assert_equal num_secondaries - 1, client.secondaries.length
    assert_equal num_secondaries - 1, client.secondary_pools.length

    #@rs.add_node(n)
  end

  def test_adding_and_removing_nodes
    client = MongoReplicaSetClient.new(build_seeds(3),
      :refresh_interval => 2, :refresh_mode => :sync)

    @rs.add_node
    sleep(4)
    client['foo']['bar'].find_one

    @conn2 = MongoReplicaSetClient.new(build_seeds(3),
      :refresh_interval => 2, :refresh_mode => :sync)

    assert @conn2.secondaries.sort == client.secondaries.sort,
      "Second connection secondaries not equal to first."
    assert_equal 3, client.secondary_pools.length
    assert_equal 3, client.secondaries.length

    config = client['admin'].command({:ismaster => 1})

    @rs.remove_secondary_node
    sleep(4)
    config = client['admin'].command({:ismaster => 1})

    assert_equal 2, client.secondary_pools.length
    assert_equal 2, client.secondaries.length
  end
=end
end
