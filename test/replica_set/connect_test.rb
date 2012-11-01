require 'test_helper'

class ConnectTest < Test::Unit::TestCase

  def setup
    ensure_cluster(:rs)
    @client = nil
  end

  def teardown
    @client.close if @client
  end

  def self.shutdown
    @@cluster.stop
    @@cluster.clobber
  end

  # To reset after (test) failure
  #     $ rm -fr data

  def step_down_command
    # Adding force=true to avoid 'no secondaries within 10 seconds of my optime' errors
    step_down_command = BSON::OrderedHash.new
    step_down_command[:replSetStepDown] = 60
    step_down_command[:force]           = true
    step_down_command
  end

  # TODO: test connect timeout.

  def test_connect_with_deprecated_multi
    #replica_host_ports = @rs.replicas.collect{|replica| [replica.host, replica.port]}
    host = @rs.replicas.first.host
    silently do
      @client = Client.multi([
        # guaranteed to have one data-holding member
        [host, @rs.replicas[0].port],
        [host, @rs.replicas[1].port],
        [host, @rs.replicas[2].port],
      ], :name => @rs.repl_set_name)
    end
    assert @client.is_a?(ReplSetClient)
    assert @client.connected?
  end

  def test_connect_bad_name
    assert_raise_error(ReplicaSetConnectionError, "-wrong") do
      @client = ReplSetClient.new(@rs.repl_set_seeds, :name => @rs.repl_set_name + "-wrong")
    end
  end

  def test_connect_with_first_secondary_node_terminated
    @rs.secondaries.first.stop

    rescue_connection_failure do
      @client = ReplSetClient.new @rs.repl_set_seeds
    end
    assert @client.connected?
  end

  def test_connect_with_last_secondary_node_terminated
    @rs.secondaries.last.stop

    rescue_connection_failure do
      @client = ReplSetClient.new @rs.repl_set_seeds
    end
    assert @client.connected?
  end

  def test_connect_with_primary_stepped_down
    @client = ReplSetClient.new @rs.repl_set_seeds
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
    @client = ReplSetClient.new @rs.repl_set_seeds
    assert @client.connected?
    @client[MONGO_TEST_DB]['bar'].save({:a => 1}, {:safe => {:w => 3}})
    assert @client[MONGO_TEST_DB]['bar'].find_one

    @rs.primary.kill(Signal.list['KILL'])

    rescue_connection_failure do
      @client[MONGO_TEST_DB]['bar'].find_one
    end
  end

  def test_save_with_primary_stepped_down
    @client = ReplSetClient.new @rs.repl_set_seeds
    assert @client.connected?

    primary = Mongo::Client.new(@client.primary_pool.host, @client.primary_pool.port)
    assert_raise Mongo::ConnectionFailure do
      primary['admin'].command(step_down_command)
    end

    rescue_connection_failure do
      @client[MONGO_TEST_DB]['bar'].save({:a => 1}, {:safe => {:w => 3}})
    end
  end

  #def test_connect_with_first_node_removed
  #  @client = ReplSetClient.new @rs.repl_set_seeds
  #  @client[MONGO_TEST_DB]['bar'].save({:a => 1}, {:safe => {:w => 3}})
  #
  #  old_primary = [@client.primary_pool.host, @client.primary_pool.port]
  #  old_primary_conn = Mongo::Client.new(*old_primary)
  #  assert_raise Mongo::ConnectionFailure do
  #    old_primary_conn['admin'].command(step_down_command)
  #  end
  #
  #  # Wait for new primary
  #  rescue_connection_failure do
  #    sleep 1 until @rs.get_node_with_state(1)
  #  end
  #
  #  new_primary = @rs.get_all_host_pairs_with_state(1).first
  #  new_primary_conn = Mongo::Client.new(*new_primary)
  #
  #  config = nil
  #
  #  # Remove old primary from replset
  #  rescue_connection_failure do
  #    config = @client['local']['system.replset'].find_one
  #  end
  #
  #  old_member = config['members'].select {|m| m['host'] == old_primary.join(':')}.first
  #  config['members'].reject! {|m| m['host'] == old_primary.join(':')}
  #  config['version'] += 1
  #
  #  begin
  #    new_primary_conn['admin'].command({'replSetReconfig' => config})
  #  rescue Mongo::ConnectionFailure
  #  end
  #
  #  # Wait for the dust to settle
  #  rescue_connection_failure do
  #    assert @client[MONGO_TEST_DB]['bar'].find_one
  #  end
  #
  #  # Make sure a new connection skips the old primary
  #  @new_conn = ReplSetClient.new @rs.repl_set_seeds
  #  @new_conn.connect
  #  new_nodes = [@new_conn.primary] + @new_conn.secondaries
  #  assert !(new_nodes).include?(old_primary)
  #
  #  # Add the old primary back
  #  config['members'] << old_member
  #  config['version'] += 1
  #
  #  begin
  #    new_primary_conn['admin'].command({'replSetReconfig' => config})
  #  rescue Mongo::ConnectionFailure
  #  end
  #end

  #def test_connect_with_hung_first_node
  #  hung_node = nil
  #  begin
  #    hung_node = IO.popen('nc -lk 127.0.0.1 29999 >/dev/null 2>&1')
  #
  #    @client = ReplSetClient.new(['localhost:29999'] + @rs.repl_set_seeds,
  #                                  :connect_timeout => 2)
  #    @client.connect
  #    assert ['localhost:29999'] != @client.primary
  #    assert !@client.secondaries.include?('localhost:29999')
  #  ensure
  #    Process.kill("KILL", hung_node.pid) if hung_node
  #  end
  #end

  def test_connect_with_connection_string
    @client = Client.from_uri("mongodb://#{@rs.replicas[0].host_port},#{@rs.replicas[1].host_port}?replicaset=#{@rs.repl_set_name}")
    assert @client.is_a?(ReplSetClient)
    assert @client.connected?
  end

  def test_connect_with_connection_string_in_env_var
    ENV['MONGODB_URI'] = "mongodb://#{@rs.replicas[0].host_port},#{@rs.replicas[1].host_port}?replicaset=#{@rs.repl_set_name}"
    @client = ReplSetClient.new
    assert @client.is_a?(ReplSetClient)
    assert_equal 2, @client.seeds.length
    assert_equal @rs.replicas[0].host, @client.seeds[0][0]
    assert_equal @rs.replicas[1].host, @client.seeds[1][0]
    assert_equal @rs.replicas[0].port, @client.seeds[0][1]
    assert_equal @rs.replicas[1].port, @client.seeds[1][1]
    assert_equal @rs.repl_set_name, @client.replica_set_name
    assert @client.connected?
  end

  def test_connect_with_connection_string_in_implicit_mongodb_uri
    ENV['MONGODB_URI'] = "mongodb://#{@rs.replicas[0].host_port},#{@rs.replicas[1].host_port}?replicaset=#{@rs.repl_set_name}"
    @client = Client.from_uri
    assert @client.is_a?(ReplSetClient)
    assert_equal 2, @client.seeds.length
    assert_equal @rs.replicas[0].host, @client.seeds[0][0]
    assert_equal @rs.replicas[1].host, @client.seeds[1][0]
    assert_equal @rs.replicas[0].port, @client.seeds[0][1]
    assert_equal @rs.replicas[1].port, @client.seeds[1][1]
    assert_equal @rs.repl_set_name, @client.replica_set_name
    assert @client.connected?
  end

  def test_connect_with_new_seed_format
    @client = ReplSetClient.new @rs.repl_set_seeds
    assert @client.connected?
  end

  def test_connect_with_old_seed_format
    silently do
      @client = ReplSetClient.new(@rs.replicas[0].host_port_a, @rs.replicas[1].host_port_a)
    end
    assert @client.connected?
  end

  def test_connect_with_full_connection_string
    @client = Client.from_uri("mongodb://#{@rs.replicas[0].host_port},#{@rs.replicas[1].host_port}?replicaset=#{@rs.repl_set_name};safe=true;w=2;fsync=true;slaveok=true")
    assert @client.is_a?(ReplSetClient)
    assert @client.connected?
    assert_equal 2, @client.safe[:w]
    assert @client.safe[:fsync]
    assert @client.read_pool
  end

  def test_connect_with_full_connection_string_in_env_var
    ENV['MONGODB_URI'] = "mongodb://#{@rs.replicas[0].host_port},#{@rs.replicas[1].host_port}?replicaset=#{@rs.repl_set_name};safe=true;w=2;fsync=true;slaveok=true"
    @client = ReplSetClient.new
    assert @client.is_a?(ReplSetClient)
    assert @client.connected?
    assert_equal 2, @client.safe[:w]
    assert @client.safe[:fsync]
    assert @client.read_pool
  end

  def test_connect_options_override_env_var
    ENV['MONGODB_URI'] = "mongodb://#{@rs.replicas[0].host_port},#{@rs.replicas[1].host_port}?replicaset=#{@rs.repl_set_name};safe=true;w=2;fsync=true;slaveok=true"
    @client = ReplSetClient.new({:safe => false})
    assert @client.is_a?(ReplSetClient)
    assert @client.connected?
    assert_equal @client.safe, false
  end

end
