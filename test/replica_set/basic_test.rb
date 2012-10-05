require 'test_helper'

class BasicTest < Test::Unit::TestCase

  def setup
    ensure_cluster(:rs)
  end

  def self.shutdown
    @@cluster.stop
    @@cluster.clobber
  end

  # TODO member.primary? ==> true
  # To reset after (test) failure
  #     $ killall mongod; rm -fr rs

  def test_connect
    conn = Mongo::ReplSetConnection.new(@rs.repl_set_seeds, :name => @rs.repl_set_name)
    assert conn.connected?
    assert_equal @rs.primary, conn.primary.join(':')
    assert_equal @rs.secondaries.sort, conn.secondaries.collect{|s| s.join(':')}.sort
    assert_equal @rs.arbiters.sort, conn.arbiters.collect{|s| s.join(':')}.sort
    conn.close

    conn = Mongo::ReplSetConnection.new(@rs.repl_set_seeds_old, :name => @rs.repl_set_name)
    assert conn.connected?
    conn.close
  end

  def test_multiple_concurrent_replica_set_connection
    conn1 = ReplSetConnection.new(@rs.repl_set_seeds, :name => @rs.repl_set_name)
    conn2 = ReplSetConnection.new(@rs.repl_set_seeds, :name => @rs.repl_set_name)
    assert conn1.connected?
    assert conn2.connected?
    assert conn1.manager != conn2.manager
    assert conn1.local_manager != conn2.local_manager
    conn1.close
    conn2.close
  end

  def test_cache_original_seed_nodes
    host = @rs.servers.first.host
    seeds = @rs.repl_set_seeds << "#{host}:19356"
    conn = ReplSetConnection.new(seeds, :name => @rs.repl_set_name)
    assert conn.connected?
    assert conn.seeds.include?([host, 19356]), "Original seed nodes not cached!"
    assert_equal [host, 19356], conn.seeds.last, "Original seed nodes not cached!"
    conn.close
  end

  def test_accessors
    seeds = @rs.repl_set_seeds
    args = {:name => @rs.repl_set_name}
    conn = ReplSetConnection.new(seeds, args)
    assert_equal @rs.primary, [conn.host, conn.port].join(':')
    assert_equal conn.host, conn.primary_pool.host
    assert_equal conn.port, conn.primary_pool.port
    assert_equal 2, conn.secondaries.length
    assert_equal 2, conn.arbiters.length
    assert_equal 2, conn.secondary_pools.length
    assert_equal @rs.repl_set_name, conn.replica_set_name
    assert conn.secondary_pools.include?(conn.read_pool(:secondary))
    assert_equal 90, conn.refresh_interval
    assert_equal conn.refresh_mode, false
    conn.close
  end

  context "Socket pools" do
    context "checking out writers" do
      setup do
        seeds = @rs.repl_set_seeds
        args = {:name => @rs.repl_set_name}
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
        seeds = @rs.repl_set_seeds
        args = {:name => @rs.repl_set_name}
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
