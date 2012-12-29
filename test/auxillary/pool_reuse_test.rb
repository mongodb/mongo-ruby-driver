require 'test_helper'
require 'mongo'

class PoolReuseTest < Test::Unit::TestCase
  include Mongo

  def count_open_file_handles
    @conn["admin"].command(:serverStatus => 1)["connections"]["current"]
  end

  def teardown
    @@cluster.stop if @@cluster
  end

  def test_pool_resources_are_reused
    ensure_cluster(:rs)
    @conn = MongoReplicaSetClient.new(["%s:%s" % [@rs.primary.host, @rs.primary.port]])

    handles_before_refresh = count_open_file_handles
    10.times do
      @conn.hard_refresh!
    end
    assert_equal handles_before_refresh, count_open_file_handles
  end

  def test_pool_connectability_after_cycling_members
    ensure_cluster(:rs, :replicas => 2, :arbiters => 1)
    conn = MongoReplicaSetClient.new(["%s:%s" % [@rs.primary.host, @rs.primary.port]])
    db   = conn['sample-db']

    assert_equal db.collection_names, []
    @@cluster.primary.stop

    rescue_connection_failure do
      db.collection_names
    end

    # We should be reconnected to the new master now
    assert_equal db.collection_names, []

    # Start up the old primary
    @@cluster.restart

    # Stop the new primary
    @@cluster.primary.stop

    # Wait for primary failover
    rescue_connection_failure do
      db.collection_names
    end

    # Reconnect and verify that we can read again
    assert_equal db.collection_names, []
  end
end