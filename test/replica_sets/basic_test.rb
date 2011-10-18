$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require './test/replica_sets/rs_test_helper'

class ConnectTest < Test::Unit::TestCase
  include Mongo

  def teardown
    RS.restart_killed_nodes
    @conn.close if defined?(@conn) && @conn
  end

  def test_connect
    @conn = ReplSetConnection.new([RS.host, RS.ports[1]], [RS.host, RS.ports[0]],
      [RS.host, RS.ports[2]], :name => RS.name)
    assert @conn.connected?

    assert_equal RS.primary, @conn.primary
    assert_equal RS.secondaries.sort, @conn.secondaries.sort
    assert_equal RS.arbiters.sort, @conn.arbiters.sort

    @conn = ReplSetConnection.new([RS.host, RS.ports[1]], [RS.host, RS.ports[0]],
      :name => RS.name)
    assert @conn.connected?
  end

  def test_accessors
    seeds = [RS.host, RS.ports[0]], [RS.host, RS.ports[1]],
      [RS.host, RS.ports[2]]
    args = seeds << {:name => RS.name}
    @conn = ReplSetConnection.new(*args)

    assert_equal @conn.host, RS.primary[0]
    assert_equal @conn.port, RS.primary[1]
    assert_equal @conn.host, @conn.primary_pool.host
    assert_equal @conn.port, @conn.primary_pool.port
    assert_equal @conn.nodes, @conn.seeds
    assert_equal 2, @conn.secondaries.length
    assert_equal 2, @conn.arbiters.length
    assert_equal 2, @conn.secondary_pools.length
    assert_equal RS.name, @conn.replica_set_name
    assert @conn.secondary_pools.include?(@conn.read_pool)
    assert_equal seeds.sort {|a,b| a[1] <=> b[1]},
      @conn.seeds.sort {|a,b| a[1] <=> b[1]}
    assert_equal 5, @conn.tag_map.keys.length
    assert_equal 90, @conn.refresh_interval
    assert_equal @conn.refresh_mode, :sync
  end
end
