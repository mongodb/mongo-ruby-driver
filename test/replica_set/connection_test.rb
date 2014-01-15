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

class ReplicaSetConnectionTest < Test::Unit::TestCase

  def setup
    ensure_cluster(:rs)
  end

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

  def test_connect_with_connection_string
    @connection = Connection.from_uri("mongodb://#{@rs.repl_set_seeds_uri}?replicaset=#{@rs.repl_set_name}")
    assert !@connection.nil?
    assert @connection.connected?
  end

  def test_connect_with_connection_string_in_env_var
    uri = "mongodb://#{@rs.repl_set_seeds_uri}?replicaset=#{@rs.repl_set_name}"
    with_preserved_env_uri(uri) do
      @connection = ReplSetConnection.new
      assert !@connection.nil?
      assert_equal 3, @connection.seeds.length
      assert_equal @rs.replicas[0].host, @connection.seeds[0][0]
      assert_equal @rs.replicas[1].host, @connection.seeds[1][0]
      assert_equal @rs.replicas[2].host, @connection.seeds[2][0]
      assert_equal @rs.replicas[0].port, @connection.seeds[0][1]
      assert_equal @rs.replicas[1].port, @connection.seeds[1][1]
      assert_equal @rs.replicas[2].port, @connection.seeds[2][1]
      assert_equal @rs.repl_set_name, @connection.replica_set_name
      assert @connection.connected?
    end
  end

  def test_connect_with_connection_string_in_implicit_mongodb_uri
    uri = "mongodb://#{@rs.repl_set_seeds_uri}?replicaset=#{@rs.repl_set_name}"
    with_preserved_env_uri(uri) do
      @connection = Connection.from_uri
      assert !@connection.nil?
      assert_equal 3, @connection.seeds.length
      assert_equal @rs.replicas[0].host, @connection.seeds[0][0]
      assert_equal @rs.replicas[1].host, @connection.seeds[1][0]
      assert_equal @rs.replicas[2].host, @connection.seeds[2][0]
      assert_equal @rs.replicas[0].port, @connection.seeds[0][1]
      assert_equal @rs.replicas[1].port, @connection.seeds[1][1]
      assert_equal @rs.replicas[2].port, @connection.seeds[2][1]
      assert_equal @rs.repl_set_name, @connection.replica_set_name
      assert @connection.connected?
    end
  end

  def test_connect_with_new_seed_format
    @connection = ReplSetConnection.new @rs.repl_set_seeds
    assert @connection.connected?
  end

  def test_connect_with_old_seed_format
    silently do
      @connection = ReplSetConnection.new(@rs.repl_set_seeds_old)
    end
    assert @connection.connected?
  end

  def test_connect_with_full_connection_string
    @connection = Connection.from_uri("mongodb://#{@rs.repl_set_seeds_uri}?replicaset=#{@rs.repl_set_name};safe=true;w=2;fsync=true;slaveok=true")
    assert !@connection.nil?
    assert @connection.connected?
    assert_equal 2, @connection.write_concern[:w]
    assert @connection.write_concern[:fsync]
    assert @connection.read_pool
  end

  def test_connect_with_full_connection_string_in_env_var
    uri = "mongodb://#{@rs.repl_set_seeds_uri}?replicaset=#{@rs.repl_set_name};safe=true;w=2;fsync=true;slaveok=true"
    with_preserved_env_uri(uri) do
      @connection = ReplSetConnection.new
      assert !@connection.nil?
      assert @connection.connected?
      assert_equal 2, @connection.write_concern[:w]
      assert @connection.write_concern[:fsync]
      assert @connection.read_pool
    end
  end

  def test_connect_options_override_env_var
    uri = "mongodb://#{@rs.repl_set_seeds_uri}?replicaset=#{@rs.repl_set_name};safe=true;w=2;fsync=true;slaveok=true"
    with_preserved_env_uri(uri) do
      @connection = ReplSetConnection.new({:safe => {:w => 1}})
      assert !@connection.nil?
      assert @connection.connected?
      assert_equal 1, @connection.write_concern[:w]
    end
  end

end
