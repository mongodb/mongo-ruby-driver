$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require './test/replica_sets/rs_test_helper'

class ReplicaSetInsertTest < Test::Unit::TestCase

  def setup
    ensure_rs
    @conn = ReplSetConnection.new([TEST_HOST, @rs.ports[0]],
                                  [TEST_HOST, @rs.ports[1]], [TEST_HOST, @rs.ports[2]])
    @db = @conn.db(MONGO_TEST_DB)
    @db.drop_collection("test-sets")
    @coll = @db.collection("test-sets")
  end

  def teardown
    @rs.restart_killed_nodes
    @conn.close if @conn
  end

  def test_insert
    @coll.save({:a => 20}, :safe => true)

    @rs.kill_primary

    rescue_connection_failure do
      @coll.save({:a => 30}, :safe => true)
    end

    @coll.save({:a => 40}, :safe => true)
    @coll.save({:a => 50}, :safe => true)
    @coll.save({:a => 60}, :safe => true)
    @coll.save({:a => 70}, :safe => true)

    # Restart the old master and wait for sync
    @rs.restart_killed_nodes
    sleep(1)
    results = []

    rescue_connection_failure do
      @coll.find.each {|r| results << r}
      [20, 30, 40, 50, 60, 70].each do |a|
        assert results.any? {|r| r['a'] == a}, "Could not find record for a => #{a}"
      end
    end

    @coll.save({:a => 80}, :safe => true)
    @coll.find.each {|r| results << r}
    [20, 30, 40, 50, 60, 70, 80].each do |a|
      assert results.any? {|r| r['a'] == a}, "Could not find record for a => #{a} on second find"
    end
  end

end
