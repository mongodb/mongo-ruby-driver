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

class ReplicaSetBasicTest < Test::Unit::TestCase

  def setup
    ensure_cluster(:rs)
  end

  def test_connect
    client = MongoReplicaSetClient.new(@rs.repl_set_seeds, :name => @rs.repl_set_name)
    assert client.connected?
    assert_equal @rs.primary_name, client.primary.join(':')
    assert_equal @rs.secondary_names.sort, client.secondaries.collect{|s| s.join(':')}.sort
    assert_equal @rs.arbiter_names.sort, client.arbiters.collect{|s| s.join(':')}.sort
    client.close

    silently do
      client = MongoReplicaSetClient.new(@rs.repl_set_seeds_old, :name => @rs.repl_set_name)
    end

    assert client.connected?
    client.close
  end

  def test_safe_option
    client = MongoReplicaSetClient.new(@rs.repl_set_seeds, :name => @rs.repl_set_name)
    assert client.connected?
    assert client.write_concern[:w] > 0
    client.close
    client = MongoReplicaSetClient.new(@rs.repl_set_seeds, :name => @rs.repl_set_name, :w => 0)
    assert client.connected?
    assert client.write_concern[:w] < 1
    client.close
    client = MongoReplicaSetClient.new(@rs.repl_set_seeds, :name => @rs.repl_set_name, :w => 2)
    assert client.connected?
    assert client.write_concern[:w] > 0
    client.close
  end

  def test_multiple_concurrent_replica_set_connection
    client1 = MongoReplicaSetClient.new(@rs.repl_set_seeds, :name => @rs.repl_set_name)
    client2 = MongoReplicaSetClient.new(@rs.repl_set_seeds, :name => @rs.repl_set_name)
    assert client1.connected?
    assert client2.connected?
    assert client1.manager != client2.manager
    assert client1.local_manager != client2.local_manager
    client1.close
    client2.close
  end

  def test_cache_original_seed_nodes
    host = @rs.servers.first.host
    seeds = @rs.repl_set_seeds << "#{host}:19356"
    client = MongoReplicaSetClient.new(seeds, :name => @rs.repl_set_name)
    assert client.connected?
    assert client.seeds.include?([host, 19356]), "Original seed nodes not cached!"
    assert_equal [host, 19356], client.seeds.last, "Original seed nodes not cached!"
    client.close
  end

  def test_accessors
    seeds = @rs.repl_set_seeds
    args = {:name => @rs.repl_set_name}
    client = MongoReplicaSetClient.new(seeds, args)
    assert_equal @rs.primary_name, [client.host, client.port].join(':')
    assert_equal client.host, client.primary_pool.host
    assert_equal client.port, client.primary_pool.port
    assert_equal 2, client.secondaries.length
    assert_equal 2, client.secondary_pools.length
    assert_equal @rs.repl_set_name, client.replica_set_name
    assert client.secondary_pools.include?(client.read_pool({:mode => :secondary}))
    assert_equal 90, client.refresh_interval
    assert_equal client.refresh_mode, false
    client.close
  end

  def test_write_commands_and_operations
    seeds = @rs.repl_set_seeds
    args = {:name => @rs.repl_set_name}
    @client = MongoReplicaSetClient.new(seeds, args)
    @coll = @client[TEST_DB]['test-write-commands-and-operations']
    with_write_commands_and_operations(@client) do
      @coll.remove
      @coll.insert({:foo => "bar"})
      assert_equal(1, @coll.count)
    end
  end

  def test_wnote_does_not_raise_exception_with_err_nil
    @client = MongoReplicaSetClient.new(@rs.repl_set_seeds, :name => @rs.repl_set_name)
    if @client.server_version < '2.5.5'
      @coll = @client[TEST_DB]['test-wnote']
      begin
        result = @coll.remove({:foo => 1}, :w => 2)
      rescue => ex
        assert(false, "should not raise an exception for a wnote response field from a remove that does not match any documents")
      end
      assert_nil result["err"]
      assert_true result.has_key?("wnote")
    end
  end

  context "Socket pools" do
    context "checking out writers" do
      setup do
        seeds = @rs.repl_set_seeds
        args = {:name => @rs.repl_set_name}
        @client = MongoReplicaSetClient.new(seeds, args)
        @coll = @client[TEST_DB]['test-connection-exceptions']
      end

      should "close the connection on send_message for major exceptions" do
        with_write_operations(@client) do # explicit even if w 0 maps to write operations
          @client.expects(:checkout_writer).raises(SystemStackError)
          @client.expects(:close)
          begin
            @coll.insert({:foo => "bar"}, :w => 0)
          rescue SystemStackError
          end
        end
      end

      should "close the connection on send_write_command for major exceptions" do
        with_write_commands(@client) do
          @client.expects(:checkout_reader).raises(SystemStackError)
          @client.expects(:close)
          begin
            @coll.insert({:foo => "bar"})
          rescue SystemStackError
          end
        end
      end

      should "close the connection on send_message_with_gle for major exceptions" do
        with_write_operations(@client) do
          @client.expects(:checkout_writer).raises(SystemStackError)
          @client.expects(:close)
          begin
            @coll.insert({:foo => "bar"})
          rescue SystemStackError
          end
        end
      end

      should "close the connection on receive_message for major exceptions" do
        @client.expects(:checkout_reader).raises(SystemStackError)
        @client.expects(:close)
        begin
          @coll.find({}, :read => :primary).next
        rescue SystemStackError
        end
      end
    end

    context "checking out readers" do
      setup do
        seeds = @rs.repl_set_seeds
        args = {:name => @rs.repl_set_name}
        @client = MongoReplicaSetClient.new(seeds, args)
        @coll = @client[TEST_DB]['test-connection-exceptions']
      end

      should "close the connection on receive_message for major exceptions" do
        @client.expects(:checkout_reader).raises(SystemStackError)
        @client.expects(:close)
        begin
          @coll.find({}, :read => :secondary).next
        rescue SystemStackError
        end
      end
    end
  end

end
