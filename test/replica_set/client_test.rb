require 'test_helper'

class ClientTest < Test::Unit::TestCase

  def setup
    ensure_cluster(:rs)
    @client = nil
  end

  def teardown
    @client.close if @client
  end

  def test_reconnection
    @client = MongoReplicaSetClient.new @rs.repl_set_seeds
    assert @client.connected?

    manager = @client.local_manager

    @client.close
    assert !@client.connected?
    assert !@client.local_manager

    @client.connect
    assert @client.connected?
    assert_equal @client.local_manager, manager
  end

  # TODO: test connect timeout.

  def test_connect_with_deprecated_multi
    silently do
      # guaranteed to have one data-holding member
      @client = MongoClient.multi(@rs.repl_set_seeds_old, :name => @rs.repl_set_name)
    end
    assert !@client.nil?
    assert @client.connected?
  end

  def test_connect_bad_name
    assert_raise_error(ReplicaSetConnectionError, "-wrong") do
      @client = MongoReplicaSetClient.new(@rs.repl_set_seeds, :name => @rs.repl_set_name + "-wrong")
    end
  end

  def test_connect_with_first_secondary_node_terminated
    @rs.secondaries.first.stop

    rescue_connection_failure do
      @client = MongoReplicaSetClient.new @rs.repl_set_seeds
    end
    assert @client.connected?
  end

  def test_connect_with_last_secondary_node_terminated
    @rs.secondaries.last.stop

    rescue_connection_failure do
      @client = MongoReplicaSetClient.new @rs.repl_set_seeds
    end
    assert @client.connected?
  end

  def test_connect_with_primary_stepped_down
    @client = MongoReplicaSetClient.new @rs.repl_set_seeds
    @client[MONGO_TEST_DB]['bar'].save({:a => 1}, {:w => 3})
    assert @client[MONGO_TEST_DB]['bar'].find_one

    primary = Mongo::MongoClient.new(*@client.primary)
    assert_raise Mongo::ConnectionFailure do
      primary['admin'].command(step_down_command)
    end
    assert @client.connected?

    rescue_connection_failure do
      @client[MONGO_TEST_DB]['bar'].find_one
    end
    @client[MONGO_TEST_DB]['bar'].find_one
  end

  def test_connect_with_primary_killed
    @client = MongoReplicaSetClient.new @rs.repl_set_seeds
    assert @client.connected?
    @client[MONGO_TEST_DB]['bar'].save({:a => 1}, {:w => 3})
    assert @client[MONGO_TEST_DB]['bar'].find_one

    @rs.primary.kill(Signal.list['KILL'])

    sleep(3)

    rescue_connection_failure do
      @client[MONGO_TEST_DB]['bar'].find_one
    end
    @client[MONGO_TEST_DB]['bar'].find_one
  end

  def test_save_with_primary_stepped_down
    @client = MongoReplicaSetClient.new @rs.repl_set_seeds
    assert @client.connected?

    primary = Mongo::MongoClient.new(*@client.primary)
    assert_raise Mongo::ConnectionFailure do
      primary['admin'].command(step_down_command)
    end

    rescue_connection_failure do
      @client[MONGO_TEST_DB]['bar'].save({:a => 1}, {:w => 2})
    end
    @client[MONGO_TEST_DB]['bar'].find_one
  end

  #def test_connect_with_first_node_removed
  #  @client = MongoReplicaSetClient.new @rs.repl_set_seeds
  #  @client[MONGO_TEST_DB]['bar'].save({:a => 1}, {:w => 3})
  #
  #  old_primary = [@client.primary_pool.host, @client.primary_pool.port]
  #  old_primary_conn = Mongo::MongoClient.new(*old_primary)
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
  #  new_primary_conn = Mongo::MongoClient.new(*new_primary)
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
  #  @new_conn = MongoReplicaSetClient.new @rs.repl_set_seeds
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
  #    @client = MongoReplicaSetClient.new(['localhost:29999'] + @rs.repl_set_seeds,
  #                                  :connect_timeout => 2)
  #    @client.connect
  #    assert ['localhost:29999'] != @client.primary
  #    assert !@client.secondaries.include?('localhost:29999')
  #  ensure
  #    Process.kill("KILL", hung_node.pid) if hung_node
  #  end
  #end

  def test_connect_with_connection_string
    @client = MongoClient.from_uri("mongodb://#{@rs.replicas[0].host_port},#{@rs.replicas[1].host_port}?replicaset=#{@rs.repl_set_name}")
    assert !@client.nil?
    assert @client.connected?
  end

  def test_connect_with_connection_string_in_env_var
    ENV['MONGODB_URI'] = "mongodb://#{@rs.replicas[0].host_port},#{@rs.replicas[1].host_port}?replicaset=#{@rs.repl_set_name}"
    @client = MongoReplicaSetClient.new
    assert !@client.nil?
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
    @client = MongoClient.from_uri
    assert !@client.nil?
    assert_equal 2, @client.seeds.length
    assert_equal @rs.replicas[0].host, @client.seeds[0][0]
    assert_equal @rs.replicas[1].host, @client.seeds[1][0]
    assert_equal @rs.replicas[0].port, @client.seeds[0][1]
    assert_equal @rs.replicas[1].port, @client.seeds[1][1]
    assert_equal @rs.repl_set_name, @client.replica_set_name
    assert @client.connected?
  end

  def test_connect_with_new_seed_format
    @client = MongoReplicaSetClient.new @rs.repl_set_seeds
    assert @client.connected?
  end

  def test_connect_with_old_seed_format
    silently do
      @client = MongoReplicaSetClient.new(@rs.repl_set_seeds_old)
    end
    assert @client.connected?
  end

  def test_connect_with_full_connection_string
    @client = MongoClient.from_uri("mongodb://#{@rs.replicas[0].host_port},#{@rs.replicas[1].host_port}?replicaset=#{@rs.repl_set_name};w=2;fsync=true;slaveok=true")
    assert !@client.nil?
    assert @client.connected?
    assert_equal 2, @client.write_concern[:w]
    assert @client.write_concern[:fsync]
    assert @client.read_pool
  end

  def test_connect_with_full_connection_string_in_env_var
    ENV['MONGODB_URI'] = "mongodb://#{@rs.replicas[0].host_port},#{@rs.replicas[1].host_port}?replicaset=#{@rs.repl_set_name};w=2;fsync=true;slaveok=true"
    @client = MongoReplicaSetClient.new
    assert !@client.nil?
    assert @client.connected?
    assert_equal 2, @client.write_concern[:w]
    assert @client.write_concern[:fsync]
    assert @client.read_pool
  end

  def test_connect_options_override_env_var
    ENV['MONGODB_URI'] = "mongodb://#{@rs.replicas[0].host_port},#{@rs.replicas[1].host_port}?replicaset=#{@rs.repl_set_name};w=2;fsync=true;slaveok=true"
    @client = MongoReplicaSetClient.new({:w => 0})
    assert !@client.nil?
    assert @client.connected?
    assert_equal 0, @client.write_concern[:w]
  end

  def test_find_and_modify_with_secondary_read_preference
    @client = MongoReplicaSetClient.new
    collection = @client[MONGO_TEST_DB].collection('test', :read => :secondary)
    collection << { :a => 1, :processed => false}

    collection.find_and_modify(
      :query => {},
      :update => {"$set" => {:processed => true}}
    )
    assert_equal collection.find_one({}, :fields => {:_id => 0}, :read => :primary), {'a' => 1, 'processed' => true}
  end
end
