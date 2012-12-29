require 'test_helper'
require 'mongo'

class PoolReuseTest < Test::Unit::TestCase
  include Mongo

  def count_open_file_handles
    @conn["admin"].command(:serverStatus => 1)["connections"]["current"]
  end

  def setup
    ensure_cluster(:rs, :replicas => 2, :arbiters => 1)
    connect
  end

  def teardown
    @@cluster.stop if @@cluster
    @conn.close if @conn
  end

  def connect
    @conn.close if @conn
    @conn = MongoReplicaSetClient.new(["%s:%s" % [@rs.primary.host, @rs.primary.port]])
  end

  def test_pool_resources_are_reused
    handles_before_refresh = count_open_file_handles
    10.times do
      @conn.hard_refresh!
    end
    assert_equal handles_before_refresh, count_open_file_handles
  end

  def test_pool_connectability_after_cycling_members
    db   = @conn['sample-db']

    assert_equal db.collection_names, []
    old_primary = @@cluster.primary
    @conn["admin"].command step_down_command rescue nil
    old_primary.stop

    rescue_connection_failure do
      db.collection_names
    end

    # We should be reconnected to the new master now
    assert_equal db.collection_names, []

    # Start up the old primary
    old_primary.start

    # Stop the new primary
    primary = @@cluster.primary
    @conn["admin"].command step_down_command rescue nil
    primary.stop

    # Wait for primary failover
    rescue_connection_failure do
      db.collection_names
    end

    # Reconnect and verify that we can read again
    assert_equal db.collection_names, []
  end
end