# Copyright (C) 2009-2013 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'test_helper'

class ReplicaSetClientTest < Test::Unit::TestCase

  def setup
    ensure_cluster(:rs)
    @client = nil
  end

  def teardown
    @client.close if @client
  end

  def test_reconnection
    @client = MongoReplicaSetClient.from_uri(@uri)
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
    @client = MongoReplicaSetClient.from_uri(@uri)
    @client[TEST_DB]['bar'].save({:a => 1}, {:w => 3})
    assert @client[TEST_DB]['bar'].find_one

    primary = Mongo::MongoClient.new(*@client.primary)
    authenticate_client(primary)
    assert_raise Mongo::ConnectionFailure do
      perform_step_down(primary)
    end
    assert @client.connected?

    rescue_connection_failure do
      @client[TEST_DB]['bar'].find_one
    end
    @client[TEST_DB]['bar'].find_one
  end

  def test_connect_with_primary_killed
    @client = MongoReplicaSetClient.from_uri(@uri)
    assert @client.connected?
    @client[TEST_DB]['bar'].save({:a => 1}, {:w => 3})
    assert @client[TEST_DB]['bar'].find_one

    @rs.primary.kill(Signal.list['KILL'])

    sleep(3)

    rescue_connection_failure do
      @client[TEST_DB]['bar'].find_one
    end
    @client[TEST_DB]['bar'].find_one
  end

  def test_save_with_primary_stepped_down
    @client = MongoReplicaSetClient.from_uri(@uri)
    assert @client.connected?

    primary = Mongo::MongoClient.new(*@client.primary)
    authenticate_client(primary)
    assert_raise Mongo::ConnectionFailure do
      perform_step_down(primary)
    end

    rescue_connection_failure do
      @client[TEST_DB]['bar'].save({:a => 1}, {:w => 2})
    end
    @client[TEST_DB]['bar'].find_one
  end

  # def test_connect_with_first_node_removed
  #   @client = MongoReplicaSetClient.from_uri(@uri)
  #   @client[TEST_DB]['bar'].save({:a => 1}, {:w => 3})

  #   # Make sure everyone's views of optimes are caught up
  #   loop do
  #     break if @rs.repl_set_get_status.all? do |status|
  #       members = status['members']
  #       primary_optime = members.find{|m| m['state'] == 1}['optime'].seconds
  #       members.any?{|m| m['state'] == 2 && primary_optime - m['optime'].seconds < 5}
  #     end
  #     sleep 1
  #   end

  #   old_primary = [@client.primary_pool.host, @client.primary_pool.port]
  #   old_primary_conn = Mongo::MongoClient.new(*old_primary)

  #   assert_raise Mongo::ConnectionFailure do
  #     perform_step_down(old_primary_conn)
  #   end

  #   # Wait for new primary
  #   rescue_connection_failure do
  #     sleep 1 until @rs.primary
  #   end

  #   new_primary = [@rs.primary.host, @rs.primary.port]
  #   new_primary_conn = Mongo::MongoClient.new(*new_primary)

  #   assert new_primary != old_primary

  #   config = nil

  #   # Remove old primary from replset
  #   rescue_connection_failure do
  #     config = @client['local']['system.replset'].find_one
  #   end

  #   old_member = config['members'].select {|m| m['host'] == old_primary.join(':')}.first
  #   config['members'].reject! {|m| m['host'] == old_primary.join(':')}
  #   config['version'] += 1

  #   begin
  #     new_primary_conn['admin'].command({'replSetReconfig' => config})
  #   rescue Mongo::ConnectionFailure
  #   end

  #   # Wait for the dust to settle
  #   rescue_connection_failure do
  #     assert @client[TEST_DB]['bar'].find_one
  #   end

  #   begin
  #     # Make sure a new connection skips the old primary
  #     @new_conn = MongoReplicaSetClient.new @rs.repl_set_seeds
  #     @new_conn.connect
  #     new_nodes = @new_conn.secondaries + [@new_conn.primary]
  #     assert !new_nodes.include?(old_primary)
  #   ensure
  #     # Add the old primary back
  #     config['members'] << old_member
  #     config['version'] += 1

  #     begin
  #       new_primary_conn['admin'].command({'replSetReconfig' => config})
  #     rescue Mongo::ConnectionFailure
  #     end
  #   end
  # end

  def test_connect_with_hung_first_node
    hung_node = nil
    begin
      hung_node = IO.popen('nc -lk 127.0.0.1 29999 >/dev/null 2>&1')

      Timeout.timeout(3) do
        @client = MongoReplicaSetClient.new(['localhost:29999'] + @rs.repl_set_seeds,
          :connect_timeout => 2)
        @client.connect
      end
      assert ['localhost:29999'] != @client.primary
      assert !@client.secondaries.include?('localhost:29999')
    ensure
      begin
        Process.kill("KILL", hung_node.pid) if hung_node
      rescue
        # the process ended, was killed already, or the system doesn't support nc
      end
    end
  end

  def test_connect_with_connection_string
    @client = MongoClient.from_uri("mongodb://#{@rs.replicas[0].host_port},#{@rs.replicas[1].host_port}?replicaset=#{@rs.repl_set_name}")
    assert !@client.nil?
    assert @client.connected?
  end

  def test_connect_with_connection_string_in_env_var
    uri = "mongodb://#{@rs.replicas[0].host_port},#{@rs.replicas[1].host_port}?replicaset=#{@rs.repl_set_name}"
    with_preserved_env_uri(uri) do
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
  end

  def test_connect_with_connection_string_in_implicit_mongodb_uri
    uri = "mongodb://#{@rs.replicas[0].host_port},#{@rs.replicas[1].host_port}?replicaset=#{@rs.repl_set_name}"
    with_preserved_env_uri(uri) do
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
    uri = "mongodb://#{@rs.replicas[0].host_port},#{@rs.replicas[1].host_port}?replicaset=#{@rs.repl_set_name};w=2;fsync=true;slaveok=true"
    with_preserved_env_uri(uri) do
      @client = MongoReplicaSetClient.new
      assert !@client.nil?
      assert @client.connected?
      assert_equal 2, @client.write_concern[:w]
      assert @client.write_concern[:fsync]
      assert @client.read_pool
    end
  end

  def test_connect_options_override_env_var
    uri = "mongodb://#{@rs.replicas[0].host_port},#{@rs.replicas[1].host_port}?replicaset=#{@rs.repl_set_name};w=2;fsync=true;slaveok=true"
    with_preserved_env_uri(uri) do
      @client = MongoReplicaSetClient.new({:w => 0})
      assert !@client.nil?
      assert @client.connected?
      assert_equal 0, @client.write_concern[:w]
    end
  end

  def test_ipv6
    @client = MongoReplicaSetClient.new(@rs.repl_set_seeds)
    with_ipv6_enabled(@client) do
      assert MongoReplicaSetClient.new(["[::1]:#{@rs.replicas[0].port}"])
    end
  end

  def test_ipv6_with_uri
    @client = MongoReplicaSetClient.new(@rs.repl_set_seeds)
    with_ipv6_enabled(@client) do
      uri = "mongodb://[::1]:#{@rs.replicas[0].port},[::1]:#{@rs.replicas[1].port}"
      with_preserved_env_uri(uri) do
        assert MongoReplicaSetClient.new
      end
    end
  end

  def test_ipv6_with_uri_opts
    @client = MongoReplicaSetClient.new(@rs.repl_set_seeds)
    with_ipv6_enabled(@client) do
      uri = "mongodb://[::1]:#{@rs.replicas[0].port},[::1]:#{@rs.replicas[1].port}/?safe=true;"
      with_preserved_env_uri(uri) do
        assert MongoReplicaSetClient.new
      end
    end
  end

  def test_ipv6_with_different_formats
    @client = MongoReplicaSetClient.new(@rs.repl_set_seeds)
    with_ipv6_enabled(@client) do
      uri = "mongodb://[::1]:#{@rs.replicas[0].port},localhost:#{@rs.replicas[1].port}"
      with_preserved_env_uri(uri) do
        assert MongoReplicaSetClient.new
      end
    end
  end

  def test_find_and_modify_with_secondary_read_preference
    @client = MongoReplicaSetClient.from_uri(@uri)
    collection = @client[TEST_DB].collection('test', :read => :secondary)
    id = BSON::ObjectId.new
    collection << { :a => id, :processed => false }

    collection.find_and_modify(
      :query => { 'a' => id },
      :update => { "$set" => { :processed => true }}
    )
    assert_equal true, collection.find_one({ 'a' => id }, :read => :primary)['processed']
  end
end
