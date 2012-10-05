$:.unshift(File.join(File.dirname(__FILE__), '../..', 'lib'))
require 'test_helper'

class ConnectTest < Test::Unit::TestCase

  def setup
    ensure_cluster(:rs)
  end

  def self.shutdown
    @@cluster.stop
    @@cluster.clobber
  end

  # To reset after (test) failure
  #     $ killall mongod; rm -fr rs

  def step_down_command
    # Adding force=true to avoid 'no secondaries within 10 seconds of my optime' errors
    step_down_command = BSON::OrderedHash.new
    step_down_command[:replSetStepDown] = 60
    step_down_command[:force]           = true
    step_down_command
  end

  # TODO: test connect timeout.

  def test_connect_with_deprecated_multi
    host = @rs.servers.first.host
    silently do
      @conn = Connection.multi([[host, @rs.servers[0].port], [host, @rs.servers[1].port]], :name => @rs.repl_set_name)
    end
    assert @conn.is_a?(ReplSetConnection)
    assert @conn.connected?
    @conn.close
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
    @conn.close
  end

  def test_connect_with_last_secondary_node_terminated
    @rs.secondaries.last.stop

    rescue_connection_failure do
      @conn = ReplSetConnection.new @rs.repl_set_seeds
    end
    assert @conn.connected?
    @conn.close
  end

  #def test_connect_with_primary_stepped_down
  #  @conn = ReplSetConnection.new @rs.repl_set_seeds
  #  @conn[MONGO_TEST_DB]['bar'].save({:a => 1}, {:safe => {:w => 3}})
  #  assert @conn[MONGO_TEST_DB]['bar'].find_one
  #
  #  primary = Mongo::Connection.new(@conn.primary_pool.host, @conn.primary_pool.port)
  #  assert_raise Mongo::ConnectionFailure do
  #    primary['admin'].command(step_down_command)
  #  end
  #  assert @conn.connected?
  #
  #  rescue_connection_failure do
  #    @conn[MONGO_TEST_DB]['bar'].find_one
  #  end
  #  @conn.close
  #end

  def test_connect_with_primary_killed
    @conn = ReplSetConnection.new @rs.repl_set_seeds
    assert @conn.connected?
    @conn[MONGO_TEST_DB]['bar'].save({:a => 1}, {:safe => {:w => 3}})
    assert @conn[MONGO_TEST_DB]['bar'].find_one

    @rs.primary.kill(Signal.list['KILL'])

    rescue_connection_failure do
      @conn[MONGO_TEST_DB]['bar'].find_one
    end
    @conn.close
  end

  #def test_save_with_primary_stepped_down
  #  @conn = ReplSetConnection.new @rs.repl_set_seeds
  #  assert @conn.connected?
  #
  #  primary = Mongo::Connection.new(@conn.primary_pool.host, @conn.primary_pool.port)
  #  assert_raise Mongo::ConnectionFailure do
  #    primary['admin'].command(step_down_command)
  #  end
  #
  #  rescue_connection_failure do
  #    @conn[MONGO_TEST_DB]['bar'].save({:a => 1}, {:safe => {:w => 3}})
  #  end
  #  @conn.close
  #end

end
