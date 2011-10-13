$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require './test/replica_sets/rs_test_helper'

# NOTE: This test expects a replica set of three nodes to be running on RS.host,
# on ports TEST_PORT, RS.ports[1], and TEST + 2.
class ConnectTest < Test::Unit::TestCase
  include Mongo

  def teardown
    RS.restart_killed_nodes
    @conn.close if defined?(@conn) && @conn
  end

  # TODO: test connect timeout.

  def test_connect_with_deprecated_multi
    @conn = Connection.multi([[RS.host, RS.ports[0]], [RS.host, RS.ports[1]]], :name => RS.name)
    assert @conn.is_a?(ReplSetConnection)
    assert @conn.connected?
  end

  def test_connect_bad_name
    assert_raise_error(ReplicaSetConnectionError, "-wrong") do
      @conn = ReplSetConnection.new([RS.host, RS.ports[0]], [RS.host, RS.ports[1]],
        [RS.host, RS.ports[2]], :name => RS.name + "-wrong")
    end
  end

  def test_connect_with_primary_node_killed
    node = RS.kill_primary

    # Becuase we're killing the primary and trying to connect right away,
    # this is going to fail right away.
    assert_raise_error(ConnectionFailure, "Failed to connect to primary node") do
      @conn = ReplSetConnection.new([RS.host, RS.ports[0]], [RS.host, RS.ports[1]],
        [RS.host, RS.ports[2]])
    end

    # This allows the secondary to come up as a primary
    rescue_connection_failure do
      @conn = ReplSetConnection.new([RS.host, RS.ports[0]], [RS.host, RS.ports[1]],
        [RS.host, RS.ports[2]])
    end
  end

  def test_connect_with_secondary_node_killed
    node = RS.kill_secondary

    rescue_connection_failure do
      @conn = ReplSetConnection.new([RS.host, RS.ports[0]], [RS.host, RS.ports[1]],
        [RS.host, RS.ports[2]])
    end
    assert @conn.connected?
  end

  def test_connect_with_third_node_killed
    RS.kill(RS.get_node_from_port(RS.ports[2]))

    rescue_connection_failure do
      @conn = ReplSetConnection.new([RS.host, RS.ports[0]], [RS.host, RS.ports[1]],
        [RS.host, RS.ports[2]])
    end
    assert @conn.connected?
  end

  def test_connect_with_primary_stepped_down
    RS.step_down_primary

    rescue_connection_failure do
      @conn = ReplSetConnection.new([RS.host, RS.ports[0]], [RS.host, RS.ports[1]],
        [RS.host, RS.ports[2]])
    end
    assert @conn.connected?
  end

end
