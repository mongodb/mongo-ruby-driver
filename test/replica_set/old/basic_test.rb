$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require './test/replica_sets/rs_test_helper'

class BasicTest < Test::Unit::TestCase
  
  def setup
    ensure_rs
  end

  def teardown
    @rs.restart_killed_nodes
    @client.close if defined?(@conn) && @conn
  end

  def test_connect
    @client = ReplSetClient.new(build_seeds(3), :name => @rs.name)
    assert @client.connected?

    assert_equal @rs.primary, @client.primary
    assert_equal @rs.secondaries.sort, @client.secondaries.sort
    assert_equal @rs.arbiters.sort, @client.arbiters.sort

    @client = ReplSetClient.new(["#{@rs.host}:#{@rs.ports[1]}","#{@rs.host}:#{@rs.ports[0]}"],
      :name => @rs.name)
    assert @client.connected?
  end

  def test_multiple_concurrent_replica_set_connection
    @conn1 = ReplSetClient.new(build_seeds(3), :name => @rs.name)
    @conn2 = ReplSetClient.new(build_seeds(3), :name => @rs.name)
    assert @conn1.connected?
    assert @conn2.connected?

    assert @conn1.manager != @conn2.manager
    assert @conn1.local_manager != @conn2.local_manager
  end

  def test_cache_original_seed_nodes
    seeds = build_seeds(3) << "#{@rs.host}:19356"
    @client = ReplSetClient.new(seeds, :name => @rs.name)
    assert @client.connected?
    assert @client.seeds.include?([@rs.host, 19356]), "Original seed nodes not cached!"
    assert_equal [@rs.host, 19356], @client.seeds.last, "Original seed nodes not cached!"
  end

  def test_accessors
    seeds = build_seeds(3)
    args = {:name => @rs.name}
    @client = ReplSetClient.new(seeds, args)

    assert_equal @client.host, @rs.primary[0]
    assert_equal @client.port, @rs.primary[1]
    assert_equal @client.host, @client.primary_pool.host
    assert_equal @client.port, @client.primary_pool.port
    assert_equal 2, @client.secondaries.length
    assert_equal 0, @client.arbiters.length
    assert_equal 2, @client.secondary_pools.length
    assert_equal @rs.name, @client.replica_set_name
    assert @client.secondary_pools.include?(@client.read_pool(:secondary))
    assert_equal 90, @client.refresh_interval
    assert_equal @client.refresh_mode, false
  end

  context "Socket pools" do
    context "checking out writers" do
      setup do
        seeds = build_seeds(3)
        args = {:name => @rs.name}
        @con = ReplSetClient.new(seeds, args)
        @coll = @con[MONGO_TEST_DB]['test-connection-exceptions']
      end

      should "close the connection on send_message for major exceptions" do
        @con.expects(:checkout_writer).raises(SystemStackError)
        @con.expects(:close)
        begin
          @coll.insert({:foo => "bar"})
        rescue SystemStackError
        end
      end

      should "close the connection on send_message_with_safe_check for major exceptions" do
        @con.expects(:checkout_writer).raises(SystemStackError)
        @con.expects(:close)
        begin
          @coll.insert({:foo => "bar"}, :safe => true)
        rescue SystemStackError
        end
      end

      should "close the connection on receive_message for major exceptions" do
        @con.expects(:checkout_reader).raises(SystemStackError)
        @con.expects(:close)
        begin
          @coll.find({}, :read => :primary).next
        rescue SystemStackError
        end
      end
    end
    
    context "checking out readers" do
      setup do
        seeds = build_seeds(3)
        args = {:name => @rs.name}
        @con = ReplSetClient.new(seeds, args)
        @coll = @con[MONGO_TEST_DB]['test-connection-exceptions']
      end
      
      should "close the connection on receive_message for major exceptions" do
        @con.expects(:checkout_reader).raises(SystemStackError)
        @con.expects(:close)
        begin
          @coll.find({}, :read => :secondary).next
        rescue SystemStackError
        end
      end
    end
  end
end
