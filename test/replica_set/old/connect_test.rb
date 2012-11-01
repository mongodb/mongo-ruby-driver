$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require './test/replica_sets/rs_test_helper'

class ConnectTest < Test::Unit::TestCase
  def setup
    @old_mongodb_uri = ENV['MONGODB_URI']
    ensure_rs
  end

  def teardown
    @rs.restart_killed_nodes
    @client.close if defined?(@client) && @client
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
      @client = Client.multi([[@rs.host, @rs.ports[0]], [@rs.host, @rs.ports[1]]], :name => @rs.name)
    end
    assert @client.is_a?(ReplSetClient)
    assert @client.connected?
  end

  def test_connect_bad_name
    assert_raise_error(ReplicaSetConnectionError, "-wrong") do
      @client = ReplSetClient.new(build_seeds(3), :name => @rs.name + "-wrong")
    end
  end

  def test_connect_with_secondary_node_terminated
    @rs.kill_secondary

    rescue_connection_failure do
      @client = ReplSetClient.new build_seeds(3)
    end
    assert @client.connected?
  end

  def test_connect_with_third_node_terminated
    @rs.kill(@rs.get_node_from_port(@rs.ports[2]))

    rescue_connection_failure do
      @client = ReplSetClient.new build_seeds(3)
    end
    assert @client.connected?
  end

  def test_connect_with_primary_stepped_down
    @client = ReplSetClient.new build_seeds(3)
    @client[MONGO_TEST_DB]['bar'].save({:a => 1}, {:safe => {:w => 3}})
    assert @client[MONGO_TEST_DB]['bar'].find_one

    primary = Mongo::Client.new(@client.primary_pool.host, @client.primary_pool.port)
    assert_raise Mongo::ConnectionFailure do
      primary['admin'].command(step_down_command)
    end
    assert @client.connected?

    rescue_connection_failure do
      @client[MONGO_TEST_DB]['bar'].find_one
    end
  end

  def test_connect_with_primary_killed
    @client = ReplSetClient.new build_seeds(3)
    @client[MONGO_TEST_DB]['bar'].save({:a => 1}, {:safe => {:w => 3}})
    assert @client[MONGO_TEST_DB]['bar'].find_one

    @rs.kill_primary(Signal.list['KILL'])

    rescue_connection_failure do
      @client[MONGO_TEST_DB]['bar'].find_one
    end
  end

  def test_save_with_primary_stepped_down
    @client = ReplSetClient.new build_seeds(3)

    primary = Mongo::Client.new(@client.primary_pool.host, @client.primary_pool.port)
    assert_raise Mongo::ConnectionFailure do
      primary['admin'].command(step_down_command)
    end

    rescue_connection_failure do
      @client[MONGO_TEST_DB]['bar'].save({:a => 1}, {:safe => {:w => 3}})
    end
  end

  def test_save_with_primary_killed
    @client = ReplSetClient.new build_seeds(3)

    @rs.kill_primary(Signal.list['KILL'])

    rescue_connection_failure do
      @client[MONGO_TEST_DB]['bar'].save({:a => 1}, {:safe => {:w => 2}})
    end
  end

  def test_connect_with_first_node_removed
    @client = ReplSetClient.new build_seeds(3)
    @client[MONGO_TEST_DB]['bar'].save({:a => 1}, {:safe => {:w => 3}})

    old_primary = [@client.primary_pool.host, @client.primary_pool.port]
    old_primary_conn = Mongo::Client.new(*old_primary)
    assert_raise Mongo::ConnectionFailure do
      old_primary_conn['admin'].command(step_down_command)
    end

    # Wait for new primary
    rescue_connection_failure do
      sleep 1 until @rs.get_node_with_state(1)
    end

    new_primary = @rs.get_all_host_pairs_with_state(1).first
    new_primary_conn = Mongo::Client.new(*new_primary)

    config = nil

    # Remove old primary from replset
    rescue_connection_failure do
      config = @client['local']['system.replset'].find_one
    end

    old_member = config['members'].select {|m| m['host'] == old_primary.join(':')}.first
    config['members'].reject! {|m| m['host'] == old_primary.join(':')}
    config['version'] += 1

    begin
      new_primary_conn['admin'].command({'replSetReconfig' => config})
    rescue Mongo::ConnectionFailure
    end

    # Wait for the dust to settle
    rescue_connection_failure do
      assert @client[MONGO_TEST_DB]['bar'].find_one
    end

    # Make sure a new connection skips the old primary
    @new_conn = ReplSetClient.new build_seeds(3)
    @new_conn.connect
    new_nodes = [@new_conn.primary] + @new_conn.secondaries
    assert !(new_nodes).include?(old_primary)

    # Add the old primary back
    config['members'] << old_member
    config['version'] += 1

    begin
      new_primary_conn['admin'].command({'replSetReconfig' => config})
    rescue Mongo::ConnectionFailure
    end
  end

  def test_connect_with_hung_first_node
    hung_node = nil
    begin
      hung_node = IO.popen('nc -lk 127.0.0.1 29999 >/dev/null 2>&1')

      @client = ReplSetClient.new(['localhost:29999'] + build_seeds(3),
        :connect_timeout => 2)
      @client.connect
      assert ['localhost:29999'] != @client.primary
      assert !@client.secondaries.include?('localhost:29999')
    ensure
      Process.kill("KILL", hung_node.pid) if hung_node
    end
  end

  def test_connect_with_connection_string
    @client = Client.from_uri("mongodb://#{@rs.host}:#{@rs.ports[0]},#{@rs.host}:#{@rs.ports[1]}?replicaset=#{@rs.name}")
    assert @client.is_a?(ReplSetClient)
    assert @client.connected?
  end

  def test_connect_with_connection_string_in_env_var
    ENV['MONGODB_URI'] = "mongodb://#{@rs.host}:#{@rs.ports[0]},#{@rs.host}:#{@rs.ports[1]}?replicaset=#{@rs.name}"
    @client = ReplSetClient.new
    assert @client.is_a?(ReplSetClient)
    assert_equal 2, @client.seeds.length
    assert_equal @rs.host, @client.seeds[0][0]
    assert_equal @rs.host, @client.seeds[1][0]
    assert_equal @rs.ports[0], @client.seeds[0][1]
    assert_equal @rs.ports[1], @client.seeds[1][1]
    assert @client.connected?
  end

  def test_connect_with_connection_string_in_implicit_mongodb_uri
    ENV['MONGODB_URI'] = "mongodb://#{@rs.host}:#{@rs.ports[0]},#{@rs.host}:#{@rs.ports[1]}?replicaset=#{@rs.name}"
    @client = Client.from_uri
    assert @client.is_a?(ReplSetClient)
    assert_equal 2, @client.seeds.length
    assert_equal @rs.host, @client.seeds[0][0]
    assert_equal @rs.host, @client.seeds[1][0]
    assert_equal @rs.ports[0], @client.seeds[0][1]
    assert_equal @rs.ports[1], @client.seeds[1][1]
    assert_equal @rs.name, @client.replica_set_name
    assert @client.connected?
  end
  
  def test_connect_with_new_seed_format
    @client = ReplSetClient.new build_seeds(3)
    assert @client.connected?
  end
  
  def test_connect_with_old_seed_format
    silently do
      @client = ReplSetClient.new([@rs.host, @rs.ports[0]], [@rs.host, @rs.ports[1]], [@rs.host, @rs.ports[2]])
    end
    assert @client.connected?
  end

  def test_connect_with_full_connection_string
    @client = Client.from_uri("mongodb://#{@rs.host}:#{@rs.ports[0]},#{@rs.host}:#{@rs.ports[1]}?replicaset=#{@rs.name};safe=true;w=2;fsync=true;slaveok=true")
    assert @client.is_a?(ReplSetClient)
    assert @client.connected?
    assert_equal 2, @client.safe[:w]
    assert @client.safe[:fsync]
    assert @client.read_pool
  end

  def test_connect_with_full_connection_string_in_env_var
    ENV['MONGODB_URI'] = "mongodb://#{@rs.host}:#{@rs.ports[0]},#{@rs.host}:#{@rs.ports[1]}?replicaset=#{@rs.name};safe=true;w=2;fsync=true;slaveok=true"
    @client = ReplSetClient.new
    assert @client.is_a?(ReplSetClient)
    assert @client.connected?
    assert_equal 2, @client.safe[:w]
    assert @client.safe[:fsync]
    assert @client.read_pool
  end

  def test_connect_options_override_env_var
    ENV['MONGODB_URI'] = "mongodb://#{@rs.host}:#{@rs.ports[0]},#{@rs.host}:#{@rs.ports[1]}?replicaset=#{@rs.name};safe=true;w=2;fsync=true;slaveok=true"
    @client = ReplSetClient.new({:safe => false})
    assert @client.is_a?(ReplSetClient)
    assert @client.connected?
    assert_equal @client.safe, false
  end
end
