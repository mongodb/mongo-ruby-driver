$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require './test/replica_sets/rs_test_helper'

class BasicTest < Test::Unit::TestCase
  
  def setup
    ensure_rs
  end

  def teardown
    @rs.restart_killed_nodes
    @conn.close if defined?(@conn) && @conn
  end

  def test_connect
    @conn = ReplSetConnection.new(build_seeds(3), :name => @rs.name)
    assert @conn.connected?

    assert_equal @rs.primary, @conn.primary
    assert_equal @rs.secondaries.sort, @conn.secondaries.sort
    assert_equal @rs.arbiters.sort, @conn.arbiters.sort

    @conn = ReplSetConnection.new(["#{@rs.host}:#{@rs.ports[1]}","#{@rs.host}:#{@rs.ports[0]}"],
      :name => @rs.name)
    assert @conn.connected?
  end

  def test_multiple_concurrent_replica_set_connection
    @conn1 = ReplSetConnection.new(build_seeds(3), :name => @rs.name)
    @conn2 = ReplSetConnection.new(build_seeds(3), :name => @rs.name)
    assert @conn1.connected?
    assert @conn2.connected?

    assert @conn1.manager != @conn2.manager
    assert @conn1.local_manager != @conn2.local_manager
  end

  def test_cache_original_seed_nodes
    seeds = build_seeds(3) << "#{@rs.host}:19356"
    @conn = ReplSetConnection.new(seeds, :name => @rs.name)
    assert @conn.connected?
    assert @conn.seeds.include?([@rs.host, 19356]), "Original seed nodes not cached!"
    assert_equal [@rs.host, 19356], @conn.seeds.last, "Original seed nodes not cached!"
  end

  def test_accessors
    seeds = build_seeds(3)
    args = {:name => @rs.name}
    @conn = ReplSetConnection.new(seeds, args)
    @major_version = @rs.version.first

    assert_equal @conn.host, @rs.primary[0]
    assert_equal @conn.port, @rs.primary[1]
    assert_equal @conn.host, @conn.primary_pool.host
    assert_equal @conn.port, @conn.primary_pool.port
    #assert_equal @conn.nodes.sort, @conn.seeds.sort
    assert_equal 2, @conn.secondaries.length
    assert_equal 0, @conn.arbiters.length
    assert_equal 2, @conn.secondary_pools.length
    assert_equal @rs.name, @conn.replica_set_name
    assert @conn.secondary_pools.include?(@conn.read_pool)
    assert_equal 90, @conn.refresh_interval
    assert_equal @conn.refresh_mode, false
    assert_equal 5, @conn.tag_map.keys.length unless @major_version < 2
  end

  context "Socket pools" do
    context "checking out writers" do
      setup do
        seeds = build_seeds(3)
        args = {:name => @rs.name}
        @con = ReplSetConnection.new(seeds, args)
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
        @con.expects(:checkout_writer).raises(SystemStackError)
        @con.expects(:close)
        begin
          @coll.find.next
        rescue SystemStackError
        end
      end
    end
    
    context "checking out readers" do
      setup do
        seeds = build_seeds(3)
        args = {:name => @rs.name}
        @con = ReplSetConnection.new(seeds, args)
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
