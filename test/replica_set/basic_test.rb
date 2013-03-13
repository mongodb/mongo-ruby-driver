require 'test_helper'

class BasicTest < Test::Unit::TestCase

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

  context "Socket pools" do
    context "checking out writers" do
      setup do
        seeds = @rs.repl_set_seeds
        args = {:name => @rs.repl_set_name}
        @client = MongoReplicaSetClient.new(seeds, args)
        @coll = @client[MONGO_TEST_DB]['test-connection-exceptions']
      end

      should "close the connection on send_message for major exceptions" do
        @client.expects(:checkout_writer).raises(SystemStackError)
        @client.expects(:close)
        begin
          @coll.insert({:foo => "bar"})
        rescue SystemStackError
        end
      end

      should "close the connection on send_message_with_gle for major exceptions" do
        @client.expects(:checkout_writer).raises(SystemStackError)
        @client.expects(:close)
        begin
          @coll.insert({:foo => "bar"})
        rescue SystemStackError
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
        @coll = @client[MONGO_TEST_DB]['test-connection-exceptions']
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
