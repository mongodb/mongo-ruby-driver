$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require './test/replica_sets/rs_test_helper'

class ConnectTest < Test::Unit::TestCase
  def setup
    ensure_rs
  end

  def teardown
    @rs.restart_killed_nodes
    @conn.close if defined?(@conn) && @conn
  end

  # TODO: test connect timeout.

  def test_connect_with_deprecated_multi
    silently do
      @conn = Connection.multi([[@rs.host, @rs.ports[0]], [@rs.host, @rs.ports[1]]], :name => @rs.name)
    end
    assert @conn.is_a?(ReplSetConnection)
    assert @conn.connected?
  end

  def test_connect_bad_name
    assert_raise_error(ReplicaSetConnectionError, "-wrong") do
      @conn = ReplSetConnection.new(build_seeds(3), :name => @rs.name + "-wrong")
    end
  end

  def test_connect_with_primary_node_killed
    @rs.kill_primary

    # Becuase we're killing the primary and trying to connect right away,
    # this is going to fail right away.
    assert_raise_error(ConnectionFailure, "Failed to connect to primary node") do
      @conn = ReplSetConnection.new build_seeds(3)
    end

    # This allows the secondary to come up as a primary
    rescue_connection_failure do
      @conn = ReplSetConnection.new build_seeds(3)
    end
  end

  def test_connect_with_secondary_node_killed
    @rs.kill_secondary

    rescue_connection_failure do
      @conn = ReplSetConnection.new build_seeds(3)
    end
    assert @conn.connected?
  end

  def test_connect_with_third_node_killed
    @rs.kill(@rs.get_node_from_port(@rs.ports[2]))

    rescue_connection_failure do
      @conn = ReplSetConnection.new build_seeds(3)
    end
    assert @conn.connected?
  end

  def test_connect_with_primary_stepped_down
    @conn = ReplSetConnection.new build_seeds(3)
    @conn[MONGO_TEST_DB]['bar'].save({:a => 1}, {:safe => {:w => 3}})
    assert @conn[MONGO_TEST_DB]['bar'].find_one

    primary = Mongo::Connection.new(@conn.primary_pool.host, @conn.primary_pool.port)
    assert_raise Mongo::ConnectionFailure do
      primary['admin'].command({:replSetStepDown => 60})
    end
    assert @conn.connected?
    assert_raise Mongo::ConnectionFailure do
      @conn[MONGO_TEST_DB]['bar'].find_one
    end
    assert !@conn.connected?

    rescue_connection_failure do
      @conn[MONGO_TEST_DB]['bar'].find_one
    end
  end

  def test_save_with_primary_stepped_down
    @conn = ReplSetConnection.new build_seeds(3)

    primary = Mongo::Connection.new(@conn.primary_pool.host, @conn.primary_pool.port)

    # Adding force=true to avoid 'no secondaries within 10 seconds of my optime' errors
    step_down_command = BSON::OrderedHash.new
    step_down_command[:replSetStepDown] = 60
    step_down_command[:force]           = true
    assert_raise Mongo::ConnectionFailure do
      primary['admin'].command(step_down_command)
    end

    rescue_connection_failure do
      @conn[MONGO_TEST_DB]['bar'].save({:a => 1}, {:safe => {:w => 3}})
    end
  end

  def test_connect_with_connection_string
    silently do
      @conn = Connection.from_uri("mongodb://#{@rs.host}:#{@rs.ports[0]},#{@rs.host}:#{@rs.ports[1]}?replicaset=#{@rs.name}")
    end
    assert @conn.is_a?(ReplSetConnection)
    assert @conn.connected?
  end

  def test_connect_with_connection_string_in_env_var
    begin
      old_mongodb_uri = ENV['MONGODB_URI']
      ENV['MONGODB_URI'] = "mongodb://#{@rs.host}:#{@rs.ports[0]},#{@rs.host}:#{@rs.ports[1]}?replicaset=#{@rs.name}"
      silently do
        @conn = ReplSetConnection.new
      end
      assert @conn.is_a?(ReplSetConnection)
      assert @conn.connected?
    ensure
      ENV['MONGODB_URI'] = old_mongodb_uri
    end
  end

  def test_connect_with_connection_string_in_implicit_mongodb_uri
    begin
      old_mongodb_uri = ENV['MONGODB_URI']
      ENV['MONGODB_URI'] = "mongodb://#{@rs.host}:#{@rs.ports[0]},#{@rs.host}:#{@rs.ports[1]}?replicaset=#{@rs.name}"
      silently do
        @conn = Connection.from_uri
      end
      assert @conn.is_a?(ReplSetConnection)
      assert @conn.connected?
    ensure
      ENV['MONGODB_URI'] = old_mongodb_uri
    end
  end
  
  def test_connect_with_new_seed_format
    @conn = ReplSetConnection.new build_seeds(3)
    assert @conn.connected?
  end
  
  def test_connect_with_old_seed_format
    silently do
      @conn = ReplSetConnection.new([@rs.host, @rs.ports[0]], [@rs.host, @rs.ports[1]], [@rs.host, @rs.ports[2]])
    end
    assert @conn.connected?
  end

  def test_connect_with_full_connection_string
    silently do
      @conn = Connection.from_uri("mongodb://#{@rs.host}:#{@rs.ports[0]},#{@rs.host}:#{@rs.ports[1]}?replicaset=#{@rs.name};safe=true;w=2;fsync=true;slaveok=true")
    end
    assert @conn.is_a?(ReplSetConnection)
    assert @conn.connected?
    assert_equal 2, @conn.safe[:w]
    assert @conn.safe[:fsync]
    assert @conn.read_pool
  end

  def test_connect_with_full_connection_string_in_env_var
    begin
      old_mongodb_uri = ENV['MONGODB_URI']
      ENV['MONGODB_URI'] = "mongodb://#{@rs.host}:#{@rs.ports[0]},#{@rs.host}:#{@rs.ports[1]}?replicaset=#{@rs.name};safe=true;w=2;fsync=true;slaveok=true"
      silently do
        @conn = ReplSetConnection.new
      end
      assert @conn.is_a?(ReplSetConnection)
      assert @conn.connected?
      assert_equal 2, @conn.safe[:w]
      assert @conn.safe[:fsync]
      assert @conn.read_pool
    ensure
      ENV['MONGODB_URI'] = old_mongodb_uri
    end
  end
end
