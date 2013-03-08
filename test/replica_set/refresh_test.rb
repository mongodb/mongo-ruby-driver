require 'test_helper'

class ReplicaSetRefreshTest < Test::Unit::TestCase

  def setup
    ensure_cluster(:rs)
  end

  def test_connect_and_manual_refresh_with_secondary_down
    num_secondaries = @rs.secondaries.size
    client = MongoReplicaSetClient.new(@rs.repl_set_seeds, :refresh_mode => false)

    assert_equal num_secondaries, client.secondaries.size
    assert client.connected?
    assert_equal client.read_pool, client.primary_pool
    old_refresh_version = client.refresh_version

    @rs.stop_secondary

    client.refresh
    assert_equal num_secondaries - 1, client.secondaries.size
    assert client.connected?
    assert_equal client.read_pool, client.primary_pool
    assert client.refresh_version > old_refresh_version
    old_refresh_version = client.refresh_version

    # Test no changes after restart until manual refresh
    @rs.restart
    assert_equal num_secondaries - 1, client.secondaries.size
    assert client.connected?
    assert_equal client.read_pool, client.primary_pool
    assert_equal client.refresh_version, old_refresh_version

    # Refresh and ensure state
    client.refresh
    assert_equal num_secondaries, client.secondaries.size
    assert client.connected?
    assert_equal client.read_pool, client.primary_pool
    assert client.refresh_version > old_refresh_version
  end

  def test_automated_refresh_with_secondary_down
    num_secondaries = @rs.secondaries.size
    client = MongoReplicaSetClient.new(@rs.repl_set_seeds,
      :refresh_interval => 1, :refresh_mode => :sync, :read => :secondary_preferred)

    # Ensure secondaries are all recognized by client and client is connected
    assert_equal num_secondaries, client.secondaries.size
    assert client.connected?
    assert client.secondary_pools.include?(client.read_pool)
    pool = client.read_pool

    @rs.member_by_name(pool.host_string).stop
    sleep(2)

    old_refresh_version = client.refresh_version
    # Trigger synchronous refresh
    client['foo']['bar'].find_one

    assert client.connected?
    assert client.refresh_version > old_refresh_version
    assert_equal num_secondaries - 1, client.secondaries.size
    assert client.secondary_pools.include?(client.read_pool)
    assert_not_equal pool, client.read_pool

    # Restart nodes and ensure refresh interval has passed
    @rs.restart
    sleep(2)

    old_refresh_version = client.refresh_version
    # Trigger synchronous refresh
    client['foo']['bar'].find_one

    assert client.connected?
    assert client.refresh_version > old_refresh_version,
      "Refresh version hasn't changed."
    assert_equal num_secondaries, client.secondaries.size
      "No secondaries have been added."
    assert_equal num_secondaries, client.secondary_pools.size
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