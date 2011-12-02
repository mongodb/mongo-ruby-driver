$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require './test/replica_sets/rs_test_helper'

class BasicTest < Test::Unit::TestCase
  include ReplicaSetTest

  def teardown
    self.rs.restart_killed_nodes
    @conn.close if defined?(@conn) && @conn
  end

  def test_connect
    @conn = ReplSetConnection.new([self.rs.host, self.rs.ports[1]], [self.rs.host, self.rs.ports[0]],
      [self.rs.host, self.rs.ports[2]], :name => self.rs.name)
    assert @conn.connected?

    assert_equal self.rs.primary, @conn.primary
    assert_equal self.rs.secondaries.sort, @conn.secondaries.sort
    assert_equal self.rs.arbiters.sort, @conn.arbiters.sort

    @conn = ReplSetConnection.new([self.rs.host, self.rs.ports[1]], [self.rs.host, self.rs.ports[0]],
      :name => self.rs.name)
    assert @conn.connected?
  end

  def test_cache_original_seed_nodes
    @conn = ReplSetConnection.new([self.rs.host, self.rs.ports[1]], [self.rs.host, self.rs.ports[0]],
      [self.rs.host, self.rs.ports[2]], [self.rs.host, 19356], :name => self.rs.name)
    assert @conn.connected?
    assert @conn.seeds.include?([self.rs.host, 19356]), "Original seed nodes not cached!"
    assert_equal [self.rs.host, 19356], @conn.seeds.last, "Original seed nodes not cached!"
  end

  def test_accessors
    seeds = [[self.rs.host, self.rs.ports[0]], [self.rs.host, self.rs.ports[1]],
      [self.rs.host, self.rs.ports[2]]]
    args = seeds << {:name => self.rs.name}
    @conn = ReplSetConnection.new(*args)

    assert_equal @conn.host, self.rs.primary[0]
    assert_equal @conn.port, self.rs.primary[1]
    assert_equal @conn.host, @conn.primary_pool.host
    assert_equal @conn.port, @conn.primary_pool.port
    assert_equal @conn.nodes.sort, @conn.seeds.sort
    assert_equal 2, @conn.secondaries.length
    assert_equal 0, @conn.arbiters.length
    assert_equal 2, @conn.secondary_pools.length
    assert_equal self.rs.name, @conn.replica_set_name
    assert @conn.secondary_pools.include?(@conn.read_pool)
    assert_equal 5, @conn.tag_map.keys.length
    assert_equal 90, @conn.refresh_interval
    assert_equal @conn.refresh_mode, false
  end

  context "Socket pools" do
    context "checking out writers" do
      setup do
        seeds = [[self.rs.host, self.rs.ports[0]], [self.rs.host, self.rs.ports[1]],
          [self.rs.host, self.rs.ports[2]]]
        args = seeds << {:name => self.rs.name}
        @con = ReplSetConnection.new(*args)
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
