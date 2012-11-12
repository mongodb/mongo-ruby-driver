require 'test_helper'

class ReplicaSetInsertTest < Test::Unit::TestCase

  def setup
    ensure_cluster(:rs)
    @client = ReplSetClient.new @rs.repl_set_seeds
    @db = @client.db(MONGO_TEST_DB)
    @db.drop_collection("test-sets")
    @coll = @db.collection("test-sets")
  end

  def teardown
    @client.close if @conn
  end

  def self.shutdown
    @@cluster.stop
    @@cluster.clobber
  end

  def test_insert
    @coll.save({:a => 20}, :w => 2)

    @rs.primary.stop

    rescue_connection_failure do
      @coll.save({:a => 30}, :w => 2)
    end

    @coll.save({:a => 40}, :w => 2)
    @coll.save({:a => 50}, :w => 2)
    @coll.save({:a => 60}, :w => 2)
    @coll.save({:a => 70}, :w => 2)

    # Restart the old master and wait for sync
    @rs.start
    sleep(5)
    results = []

    rescue_connection_failure do
      @coll.find.each {|r| results << r}
      [20, 30, 40, 50, 60, 70].each do |a|
        assert results.any? {|r| r['a'] == a}, "Could not find record for a => #{a}"
      end
    end

    @coll.save({:a => 80}, :w => 2)
    @coll.find.each {|r| results << r}
    [20, 30, 40, 50, 60, 70, 80].each do |a|
      assert results.any? {|r| r['a'] == a}, "Could not find record for a => #{a} on second find"
    end
  end

end
