require 'test_helper'

class ConnectionTest < Test::Unit::TestCase

  def setup
    ensure_cluster(:rs)
    @connection = nil
  end

  def teardown
    @connection.close if @connection
  end

  # TODO: test connect timeout.

  def test_connect_with_deprecated_multi
    silently do
      @connection = Connection.multi(@rs.repl_set_seeds_old, :name => @rs.repl_set_name)
    end
    assert !@connection.nil?
    assert @connection.connected?
  end

  def test_connect_bad_name
    assert_raise_error(ReplicaSetConnectionError, "-wrong") do
      @connection = ReplSetConnection.new(@rs.repl_set_seeds, :safe => true, :name => @rs.repl_set_name + "-wrong")
    end
  end

  def test_connect_with_first_secondary_node_terminated
    @rs.secondaries.first.stop

    rescue_connection_failure do
      @connection = ReplSetConnection.new @rs.repl_set_seeds
    end
    assert @connection.connected?
  end

  def test_connect_with_last_secondary_node_terminated
    @rs.secondaries.last.stop

    rescue_connection_failure do
      @connection = ReplSetConnection.new @rs.repl_set_seeds
    end
    assert @connection.connected?
  end

  def test_connect_with_primary_stepped_down
    @connection = ReplSetConnection.new @rs.repl_set_seeds
    @connection[MONGO_TEST_DB]['bar'].save({:a => 1}, {:safe => {:w => 2}})
    assert @connection[MONGO_TEST_DB]['bar'].find_one

    primary = Mongo::Connection.new(@connection.primary_pool.host, @connection.primary_pool.port)
    assert_raise Mongo::ConnectionFailure do
      primary['admin'].command(step_down_command)
    end
    assert @connection.connected?

    rescue_connection_failure do
      @connection[MONGO_TEST_DB]['bar'].find_one
    end
  end

  def test_connect_with_primary_killed
    @connection = ReplSetConnection.new @rs.repl_set_seeds
    assert @connection.connected?
    @connection[MONGO_TEST_DB]['bar'].save({:a => 1}, {:safe => {:w => 1}})
    assert @connection[MONGO_TEST_DB]['bar'].find_one

    @rs.primary.kill(Signal.list['KILL'])

    rescue_connection_failure do
      @connection[MONGO_TEST_DB]['bar'].find_one
    end
  end

  def test_save_with_primary_stepped_down
    @connection = ReplSetConnection.new @rs.repl_set_seeds
    assert @connection.connected?

    primary = Mongo::Connection.new(@connection.primary_pool.host, @connection.primary_pool.port)
    assert_raise Mongo::ConnectionFailure do
      primary['admin'].command(step_down_command)
    end

    rescue_connection_failure do
      @connection[MONGO_TEST_DB]['bar'].save({:a => 1}, {:safe => {:w => 2}})
    end
  end

  #def test_connect_with_first_node_removed
  #  @connection = ReplSetConnection.new @rs.repl_set_seeds
  #  @connection[MONGO_TEST_DB]['bar'].save({:a => 1}, {:safe => {:w => 2}})
  #
  #  old_primary = [@connection.primary_pool.host, @connection.primary_pool.port]
  #  old_primary_conn = Mongo::Connection.new(*old_primary)
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
  #  new_primary_conn = Mongo::Connection.new(*new_primary)
  #
  #  config = nil
  #
  #  # Remove old primary from replset
  #  rescue_connection_failure do
  #    config = @connection['local']['system.replset'].find_one
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
  #    assert @connection[MONGO_TEST_DB]['bar'].find_one
  #  end
  #
  #  # Make sure a new connection skips the old primary
  #  @new_conn = ReplSetConnection.new @rs.repl_set_seeds
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
  #    @connection = ReplSetConnection.new(['localhost:29999'] + @rs.repl_set_seeds,
  #                                  :connect_timeout => 2)
  #    @connection.connect
  #    assert ['localhost:29999'] != @connection.primary
  #    assert !@connection.secondaries.include?('localhost:29999')
  #  ensure
  #    Process.kill("KILL", hung_node.pid) if hung_node
  #  end
  #end

  def test_connect_with_connection_string
    @connection = Connection.from_uri("mongodb://#{@rs.replicas[0].host_port},#{@rs.replicas[1].host_port}?replicaset=#{@rs.repl_set_name}")
    assert !@connection.nil?
    assert @connection.connected?
  end

  def test_connect_with_connection_string_in_env_var
    ENV['MONGODB_URI'] = "mongodb://#{@rs.replicas[0].host_port},#{@rs.replicas[1].host_port}?replicaset=#{@rs.repl_set_name}"
    @connection = ReplSetConnection.new
    assert !@connection.nil?
    assert_equal 2, @connection.seeds.length
    assert_equal @rs.replicas[0].host, @connection.seeds[0][0]
    assert_equal @rs.replicas[1].host, @connection.seeds[1][0]
    assert_equal @rs.replicas[0].port, @connection.seeds[0][1]
    assert_equal @rs.replicas[1].port, @connection.seeds[1][1]
    assert_equal @rs.repl_set_name, @connection.replica_set_name
    assert @connection.connected?
  end

  def test_connect_with_connection_string_in_implicit_mongodb_uri
    ENV['MONGODB_URI'] = "mongodb://#{@rs.replicas[0].host_port},#{@rs.replicas[1].host_port}?replicaset=#{@rs.repl_set_name}"
    @connection = Connection.from_uri
    assert !@connection.nil?
    assert_equal 2, @connection.seeds.length
    assert_equal @rs.replicas[0].host, @connection.seeds[0][0]
    assert_equal @rs.replicas[1].host, @connection.seeds[1][0]
    assert_equal @rs.replicas[0].port, @connection.seeds[0][1]
    assert_equal @rs.replicas[1].port, @connection.seeds[1][1]
    assert_equal @rs.repl_set_name, @connection.replica_set_name
    assert @connection.connected?
  end

  def test_connect_with_new_seed_format
    @connection = ReplSetConnection.new @rs.repl_set_seeds
    assert @connection.connected?
  end

  def test_connect_with_old_seed_format
    silently do
      @connection = ReplSetConnection.new([@rs.replicas[0].host_port_a, @rs.replicas[1].host_port_a])
    end
    assert @connection.connected?
  end

  def test_connect_with_full_connection_string
    @connection = Connection.from_uri("mongodb://#{@rs.replicas[0].host_port},#{@rs.replicas[1].host_port}?replicaset=#{@rs.repl_set_name};safe=true;w=2;fsync=true;slaveok=true")
    assert !@connection.nil?
    assert @connection.connected?
    assert_equal 2, @connection.write_concern[:w]
    assert @connection.write_concern[:fsync]
    assert @connection.read_pool
  end

  def test_connect_with_full_connection_string_in_env_var
    ENV['MONGODB_URI'] = "mongodb://#{@rs.replicas[0].host_port},#{@rs.replicas[1].host_port}?replicaset=#{@rs.repl_set_name};safe=true;w=2;fsync=true;slaveok=true"
    @connection = ReplSetConnection.new
    assert !@connection.nil?
    assert @connection.connected?
    assert_equal 2, @connection.write_concern[:w]
    assert @connection.write_concern[:fsync]
    assert @connection.read_pool
  end

  def test_connect_options_override_env_var
    ENV['MONGODB_URI'] = "mongodb://#{@rs.replicas[0].host_port},#{@rs.replicas[1].host_port}?replicaset=#{@rs.repl_set_name};safe=true;w=2;fsync=true;slaveok=true"
    @connection = ReplSetConnection.new({:safe => {:w => 1}})
    assert !@connection.nil?
    assert @connection.connected?
    assert_equal 1, @connection.write_concern[:w]
  end

end
