require 'test_helper'

class ReplicaSetInsertTest < Test::Unit::TestCase

  def setup
    ensure_cluster(:rs)
    @client = MongoReplicaSetClient.new @rs.repl_set_seeds
    @db = @client.db(MONGO_TEST_DB)
    @db.drop_collection("test-sets")
    @coll = @db.collection("test-sets")
  end

  def teardown
    @client.close if @conn
  end

  def test_insert
    @coll.save({:a => 20}, :w => 3)

    @rs.primary.stop

    rescue_connection_failure do
      @coll.save({:a => 30}, :w => 1)
    end

    @coll.save({:a => 40}, :w => 1)
    @coll.save({:a => 50}, :w => 1)
    @coll.save({:a => 60}, :w => 1)
    @coll.save({:a => 70}, :w => 1)

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

    @coll.save({:a => 80}, :w => 3)
    @coll.find.each {|r| results << r}
    [20, 30, 40, 50, 60, 70, 80].each do |a|
      assert results.any? {|r| r['a'] == a}, "Could not find record for a => #{a} on second find"
    end
  end

end
