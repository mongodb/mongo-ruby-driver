$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require './test/replica_sets/rs_test_helper'
require 'logger'

class ReadPreferenceTest < Test::Unit::TestCase
  include ReplicaSetTest

  def setup
    log = Logger.new("test.log")
    @conn = ReplSetConnection.new([self.rs.host, self.rs.ports[0]],
                                 [self.rs.host, self.rs.ports[1]],
          :read => :secondary, :pool_size => 50,
          :refresh_mode => false, :refresh_interval => 5, :logger => log)
    @db = @conn.db(MONGO_TEST_DB)
    @db.drop_collection("test-sets")
    col = @db['mongo-test']
  end

  def teardown
    self.rs.restart_killed_nodes
  end

  def test_read_primary
    rescue_connection_failure do
      assert !@conn.read_primary?
      assert !@conn.primary?
    end
  end

  def test_con
    assert @conn.primary_pool, "No primary pool!"
    assert @conn.read_pool, "No read pool!"
    assert @conn.primary_pool.port != @conn.read_pool.port,
      "Primary port and read port at the same!"
  end

  def test_query_secondaries
    @secondary = Connection.new(self.rs.host, @conn.read_pool.port, :slave_ok => true)
    @coll = @db.collection("test-sets", :safe => {:w => 3, :wtimeout => 20000})
    @coll.save({:a => 20})
    @coll.save({:a => 30})
    @coll.save({:a => 40})
    results = []
    queries_before = @secondary['admin'].command({:serverStatus => 1})['opcounters']['query']
    @coll.find.each {|r| results << r["a"]}
    queries_after = @secondary['admin'].command({:serverStatus => 1})['opcounters']['query']
    assert_equal 1, queries_after - queries_before
    assert results.include?(20)
    assert results.include?(30)
    assert results.include?(40)

    self.rs.kill_primary

    results = []
    rescue_connection_failure do
      puts "@coll.find().each"
      @coll.find.each {|r| results << r}
      [20, 30, 40].each do |a|
        assert results.any? {|r| r['a'] == a}, "Could not find record for a => #{a}"
      end
    end
  end

  def test_kill_primary
    @coll = @db.collection("test-sets", :safe => {:w => 3, :wtimeout => 10000})
    @coll.save({:a => 20})
    @coll.save({:a => 30})
    assert_equal 2, @coll.find.to_a.length

    # Should still be able to read immediately after killing master node
    self.rs.kill_primary
    assert_equal 2, @coll.find.to_a.length
    rescue_connection_failure do
      puts "@coll.save()"
      @coll.save({:a => 50}, :safe => {:w => 2, :wtimeout => 10000})
    end
    self.rs.restart_killed_nodes
    @coll.save({:a => 50}, :safe => {:w => 2, :wtimeout => 10000})
    assert_equal 4, @coll.find.to_a.length
  end

  def test_kill_secondary
    @coll = @db.collection("test-sets", {:safe => {:w => 3, :wtimeout => 20000}})
    @coll.save({:a => 20})
    @coll.save({:a => 30})
    assert_equal 2, @coll.find.to_a.length

    read_node = self.rs.get_node_from_port(@conn.read_pool.port)
    self.rs.kill(read_node)

    # Should fail immediately on next read
    old_read_pool_port = @conn.read_pool.port
    assert_raise ConnectionFailure do
      @coll.find.to_a.length
    end

    # Should eventually reconnect and be able to read
    rescue_connection_failure do
      length = @coll.find.to_a.length
      assert_equal 2, length
    end
    new_read_pool_port = @conn.read_pool.port
    assert old_read_pool_port != new_read_pool_port
  end

  def test_write_lots_of_data
    @coll = @db.collection("test-sets", {:safe => {:w => 2}})

    6000.times do |n|
      @coll.save({:a => n})
    end

    cursor = @coll.find()
    cursor.next
    cursor.close
  end

  # TODO: enable this once we enable reads from tags.
  # def test_query_tagged
  #   col = @db['mongo-test']

  #   col.insert({:a => 1}, :safe => {:w => 3})
  #   col.find_one({}, :read => {:db => "main"})
  #   col.find_one({}, :read => {:dc => "ny"})
  #   col.find_one({}, :read => {:dc => "sf"})

  #   assert_raise Mongo::NodeWithTagsNotFound do
  #     col.find_one({}, :read => {:foo => "bar"})
  #   end

  #   threads = []
  #   100.times do
  #     threads << Thread.new do
  #       col.find_one({}, :read => {:dc => "sf"})
  #     end
  #   end

  #   threads.each {|t| t.join }

  #   col.remove
  # end

  #def teardown
  #  self.rs.restart_killed_nodes
  #end

end
