require 'test_helper'

class ConnectTest < Test::Unit::TestCase

  def setup
    ensure_cluster(:rs)
    @conn = nil
  end

  def teardown
    @conn.close if @conn
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
    @conn = Connection.multi([
      # guaranteed to have one data-holding member
      [host, @rs.replicas[0].port],
      [host, @rs.replicas[1].port],
      [host, @rs.replicas[2].port],
    ], :name => @rs.repl_set_name)
    assert @conn.is_a?(ReplSetConnection)
    assert @conn.connected?
  end

  def test_connect_bad_name
    assert_raise_error(ReplicaSetConnectionError, "-wrong") do
      @conn = ReplSetConnection.new(@rs.repl_set_seeds, :name => @rs.repl_set_name + "-wrong")
    end
  end

  def test_connect_with_first_secondary_node_terminated
    @rs.secondaries.first.stop

    rescue_connection_failure do
      @conn = ReplSetConnection.new @rs.repl_set_seeds
    end
    assert @conn.connected?
  end

  def test_connect_with_last_secondary_node_terminated
    @rs.secondaries.last.stop

    rescue_connection_failure do
      @conn = ReplSetConnection.new @rs.repl_set_seeds
    end
    assert @conn.connected?
  end

  def test_connect_with_primary_stepped_down
    @conn = ReplSetConnection.new @rs.repl_set_seeds
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

  def test_connect_with_primary_killed
    @conn = ReplSetConnection.new @rs.repl_set_seeds
    assert @conn.connected?
    @conn[MONGO_TEST_DB]['bar'].save({:a => 1}, {:safe => {:w => 3}})
    assert @conn[MONGO_TEST_DB]['bar'].find_one

    @rs.primary.kill(Signal.list['KILL'])

    rescue_connection_failure do
      @conn[MONGO_TEST_DB]['bar'].find_one
    end
  end

  def test_save_with_primary_stepped_down
    @conn = ReplSetConnection.new @rs.repl_set_seeds
    assert @conn.connected?

    primary = Mongo::Connection.new(@conn.primary_pool.host, @conn.primary_pool.port)
    assert_raise Mongo::ConnectionFailure do
      primary['admin'].command(step_down_command)
    end

    rescue_connection_failure do
      @conn[MONGO_TEST_DB]['bar'].save({:a => 1}, {:safe => {:w => 3}})
    end
  end

  #def test_connect_with_first_node_removed
  #  @conn = ReplSetConnection.new @rs.repl_set_seeds
  #  @conn[MONGO_TEST_DB]['bar'].save({:a => 1}, {:safe => {:w => 3}})
  #
  #  old_primary = [@conn.primary_pool.host, @conn.primary_pool.port]
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
  #    config = @conn['local']['system.replset'].find_one
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
  #    assert @conn[MONGO_TEST_DB]['bar'].find_one
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
  #    @conn = ReplSetConnection.new(['localhost:29999'] + @rs.repl_set_seeds,
  #                                  :connect_timeout => 2)
  #    @conn.connect
  #    assert ['localhost:29999'] != @conn.primary
  #    assert !@conn.secondaries.include?('localhost:29999')
  #  ensure
  #    Process.kill("KILL", hung_node.pid) if hung_node
  #  end
  #end

  def test_connect_with_connection_string
    @conn = Connection.from_uri("mongodb://#{@rs.replicas[0].host_port},#{@rs.replicas[1].host_port}?replicaset=#{@rs.repl_set_name}")
    assert @conn.is_a?(ReplSetConnection)
    assert @conn.connected?
  end

  def test_connect_with_connection_string_in_env_var
    ENV['MONGODB_URI'] = "mongodb://#{@rs.replicas[0].host_port},#{@rs.replicas[1].host_port}?replicaset=#{@rs.repl_set_name}"
    @conn = ReplSetConnection.new
    assert @conn.is_a?(ReplSetConnection)
    assert_equal 2, @conn.seeds.length
    assert_equal @rs.replicas[0].host, @conn.seeds[0][0]
    assert_equal @rs.replicas[1].host, @conn.seeds[1][0]
    assert_equal @rs.replicas[0].port, @conn.seeds[0][1]
    assert_equal @rs.replicas[1].port, @conn.seeds[1][1]
    assert_equal @rs.repl_set_name, @conn.replica_set_name
    assert @conn.connected?
  end

  def test_connect_with_connection_string_in_implicit_mongodb_uri
    ENV['MONGODB_URI'] = "mongodb://#{@rs.replicas[0].host_port},#{@rs.replicas[1].host_port}?replicaset=#{@rs.repl_set_name}"
    @conn = Connection.from_uri
    assert @conn.is_a?(ReplSetConnection)
    assert_equal 2, @conn.seeds.length
    assert_equal @rs.replicas[0].host, @conn.seeds[0][0]
    assert_equal @rs.replicas[1].host, @conn.seeds[1][0]
    assert_equal @rs.replicas[0].port, @conn.seeds[0][1]
    assert_equal @rs.replicas[1].port, @conn.seeds[1][1]
    assert_equal @rs.repl_set_name, @conn.replica_set_name
    assert @conn.connected?
  end

  def test_connect_with_new_seed_format
    @conn = ReplSetConnection.new @rs.repl_set_seeds
    assert @conn.connected?
  end

  def test_connect_with_old_seed_format
    silently do
      @conn = ReplSetConnection.new(@rs.replicas[0].host_port_a, @rs.replicas[1].host_port_a)
    end
    assert @conn.connected?
  end

  def test_connect_with_full_connection_string
    @conn = Connection.from_uri("mongodb://#{@rs.replicas[0].host_port},#{@rs.replicas[1].host_port}?replicaset=#{@rs.repl_set_name};safe=true;w=2;fsync=true;slaveok=true")
    assert @conn.is_a?(ReplSetConnection)
    assert @conn.connected?
    assert_equal 2, @conn.safe[:w]
    assert @conn.safe[:fsync]
    assert @conn.read_pool
  end

  def test_connect_with_full_connection_string_in_env_var
    ENV['MONGODB_URI'] = "mongodb://#{@rs.replicas[0].host_port},#{@rs.replicas[1].host_port}?replicaset=#{@rs.repl_set_name};safe=true;w=2;fsync=true;slaveok=true"
    @conn = ReplSetConnection.new
    assert @conn.is_a?(ReplSetConnection)
    assert @conn.connected?
    assert_equal 2, @conn.safe[:w]
    assert @conn.safe[:fsync]
    assert @conn.read_pool
  end

  def test_connect_options_override_env_var
    ENV['MONGODB_URI'] = "mongodb://#{@rs.replicas[0].host_port},#{@rs.replicas[1].host_port}?replicaset=#{@rs.repl_set_name};safe=true;w=2;fsync=true;slaveok=true"
    @conn = ReplSetConnection.new({:safe => false})
    assert @conn.is_a?(ReplSetConnection)
    assert @conn.connected?
    assert_equal @conn.safe, false
  end

end
