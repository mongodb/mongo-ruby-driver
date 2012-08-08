$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require './test/replica_sets/rs_test_helper'

class ConnectTest < Test::Unit::TestCase
  def setup
    @old_mongodb_uri = ENV['MONGODB_URI']
    ensure_rs
  end

  def teardown
    @rs.restart_killed_nodes
    @conn.close if defined?(@conn) && @conn
    ENV['MONGODB_URI'] = @old_mongodb_uri
  end

  def step_down_command
    # Adding force=true to avoid 'no secondaries within 10 seconds of my optime' errors
    step_down_command = BSON::OrderedHash.new
    step_down_command[:replSetStepDown] = 60
    step_down_command[:force]           = true
    step_down_command
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
      primary['admin'].command(step_down_command)
    end
    assert @conn.connected?

    rescue_connection_failure do
      @conn[MONGO_TEST_DB]['bar'].find_one
    end
  end

  def test_save_with_primary_stepped_down
    @conn = ReplSetConnection.new build_seeds(3)

    primary = Mongo::Connection.new(@conn.primary_pool.host, @conn.primary_pool.port)
    assert_raise Mongo::ConnectionFailure do
      primary['admin'].command(step_down_command)
    end

    rescue_connection_failure do
      @conn[MONGO_TEST_DB]['bar'].save({:a => 1}, {:safe => {:w => 3}})
    end
  end

  def test_connect_with_connection_string
    @conn = Connection.from_uri("mongodb://#{@rs.host}:#{@rs.ports[0]},#{@rs.host}:#{@rs.ports[1]}?replicaset=#{@rs.name}")
    assert @conn.is_a?(ReplSetConnection)
    assert @conn.connected?
  end

  def test_connect_with_connection_string_in_env_var
    ENV['MONGODB_URI'] = "mongodb://#{@rs.host}:#{@rs.ports[0]},#{@rs.host}:#{@rs.ports[1]}?replicaset=#{@rs.name}"
    @conn = ReplSetConnection.new
    assert @conn.is_a?(ReplSetConnection)
    assert_equal 2, @conn.seeds.length
    assert_equal @rs.host, @conn.seeds[0][0]
    assert_equal @rs.host, @conn.seeds[1][0]
    assert_equal @rs.ports[0], @conn.seeds[0][1]
    assert_equal @rs.ports[1], @conn.seeds[1][1]
    assert @conn.connected?
  end

  def test_connect_with_connection_string_in_implicit_mongodb_uri
    ENV['MONGODB_URI'] = "mongodb://#{@rs.host}:#{@rs.ports[0]},#{@rs.host}:#{@rs.ports[1]}?replicaset=#{@rs.name}"
    @conn = Connection.from_uri
    assert @conn.is_a?(ReplSetConnection)
    assert_equal 2, @conn.seeds.length
    assert_equal @rs.host, @conn.seeds[0][0]
    assert_equal @rs.host, @conn.seeds[1][0]
    assert_equal @rs.ports[0], @conn.seeds[0][1]
    assert_equal @rs.ports[1], @conn.seeds[1][1]
    assert_equal @rs.name, @conn.replica_set_name
    assert @conn.connected?
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
    @conn = Connection.from_uri("mongodb://#{@rs.host}:#{@rs.ports[0]},#{@rs.host}:#{@rs.ports[1]}?replicaset=#{@rs.name};safe=true;w=2;fsync=true;slaveok=true")
    assert @conn.is_a?(ReplSetConnection)
    assert @conn.connected?
    assert_equal 2, @conn.safe[:w]
    assert @conn.safe[:fsync]
    assert @conn.read_pool
  end

  def test_connect_with_full_connection_string_in_env_var
    ENV['MONGODB_URI'] = "mongodb://#{@rs.host}:#{@rs.ports[0]},#{@rs.host}:#{@rs.ports[1]}?replicaset=#{@rs.name};safe=true;w=2;fsync=true;slaveok=true"
    @conn = ReplSetConnection.new
    assert @conn.is_a?(ReplSetConnection)
    assert @conn.connected?
    assert_equal 2, @conn.safe[:w]
    assert @conn.safe[:fsync]
    assert @conn.read_pool
  end

  def test_connect_options_override_env_var
    ENV['MONGODB_URI'] = "mongodb://#{@rs.host}:#{@rs.ports[0]},#{@rs.host}:#{@rs.ports[1]}?replicaset=#{@rs.name};safe=true;w=2;fsync=true;slaveok=true"
    @conn = ReplSetConnection.new({:safe => false})
    assert @conn.is_a?(ReplSetConnection)
    assert @conn.connected?
    assert_equal @conn.safe, false
  end
end
