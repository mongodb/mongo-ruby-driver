$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require './test/replica_sets/rs_test_helper'

class ConnectTest < Test::Unit::TestCase
  include ReplicaSetTest

  def teardown
    self.rs.restart_killed_nodes
    @conn.close if defined?(@conn) && @conn
  end

  # TODO: test connect timeout.

  def test_connect_with_deprecated_multi
    @conn = Connection.multi([[self.rs.host, self.rs.ports[0]], [self.rs.host, self.rs.ports[1]]], :name => self.rs.name)
    assert @conn.is_a?(ReplSetConnection)
    assert @conn.connected?
  end

  def test_connect_bad_name
    assert_raise_error(ReplicaSetConnectionError, "-wrong") do
      @conn = ReplSetConnection.new([self.rs.host, self.rs.ports[0]], [self.rs.host, self.rs.ports[1]],
        [self.rs.host, self.rs.ports[2]], :name => self.rs.name + "-wrong")
    end
  end

  def test_connect_with_primary_node_killed
    node = self.rs.kill_primary

    # Becuase we're killing the primary and trying to connect right away,
    # this is going to fail right away.
    assert_raise_error(ConnectionFailure, "Failed to connect to primary node") do
      @conn = ReplSetConnection.new([self.rs.host, self.rs.ports[0]], [self.rs.host, self.rs.ports[1]],
        [self.rs.host, self.rs.ports[2]])
    end

    # This allows the secondary to come up as a primary
    rescue_connection_failure do
      @conn = ReplSetConnection.new([self.rs.host, self.rs.ports[0]], [self.rs.host, self.rs.ports[1]],
        [self.rs.host, self.rs.ports[2]])
    end
  end

  def test_connect_with_secondary_node_killed
    node = self.rs.kill_secondary

    rescue_connection_failure do
      @conn = ReplSetConnection.new([self.rs.host, self.rs.ports[0]], [self.rs.host, self.rs.ports[1]],
        [self.rs.host, self.rs.ports[2]])
    end
    assert @conn.connected?
  end

  def test_connect_with_third_node_killed
    self.rs.kill(self.rs.get_node_from_port(self.rs.ports[2]))

    rescue_connection_failure do
      @conn = ReplSetConnection.new([self.rs.host, self.rs.ports[0]], [self.rs.host, self.rs.ports[1]],
        [self.rs.host, self.rs.ports[2]])
    end
    assert @conn.connected?
  end

  def test_connect_with_primary_stepped_down
    @conn = ReplSetConnection.new([self.rs.host, self.rs.ports[0]], [self.rs.host, self.rs.ports[1]],
      [self.rs.host, self.rs.ports[2]])
    @conn[MONGO_TEST_DB]['bar'].save({:a => 1}, {:safe => {:w => 3}})
    assert @conn[MONGO_TEST_DB]['bar'].find_one

    primary = Mongo::Connection.new(@conn.primary_pool.host, @conn.primary_pool.port)
    primary['admin'].command({:replSetStepDown => 60})
    assert @conn.connected?
    assert_raise_error Mongo::ConnectionFailure, "not master" do
      @conn[MONGO_TEST_DB]['bar'].find_one
    end
    assert !@conn.connected?

    rescue_connection_failure do
      @conn[MONGO_TEST_DB]['bar'].find_one
    end
  end

  def test_connect_with_connection_string
    @conn = Connection.from_uri("mongodb://#{self.rs.host}:#{self.rs.ports[0]},#{self.rs.host}:#{self.rs.ports[1]}?replicaset=#{self.rs.name}")
    assert @conn.is_a?(ReplSetConnection)
    assert @conn.connected?
  end

  def test_connect_with_full_connection_string
    @conn = Connection.from_uri("mongodb://#{self.rs.host}:#{self.rs.ports[0]},#{self.rs.host}:#{self.rs.ports[1]}?replicaset=#{self.rs.name};safe=true;w=2;fsync=true;slaveok=true")
    assert @conn.is_a?(ReplSetConnection)
    assert @conn.connected?
    assert_equal 2, @conn.safe[:w]
    assert @conn.safe[:fsync]
    assert @conn.read_pool
  end
end
