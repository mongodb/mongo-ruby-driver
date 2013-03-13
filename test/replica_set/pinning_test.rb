require 'test_helper'

class ReplicaSetPinningTest < Test::Unit::TestCase
  def setup
    ensure_cluster(:rs)
    @client = MongoReplicaSetClient.new(@rs.repl_set_seeds, :name => @rs.repl_set_name)
    @db = @client.db(MONGO_TEST_DB)
    @coll = @db.collection("test-sets")
    @coll.insert({:a => 1})
  end

  def test_unpinning
    # pin primary
    @coll.find_one
    assert_equal @client.pinned_pool[:pool], @client.primary_pool

    # pin secondary
    @coll.find_one({}, :read => :secondary_preferred)
    assert @client.secondary_pools.include? @client.pinned_pool[:pool]

    # repin primary
    @coll.find_one({}, :read => :primary_preferred)
    assert_equal @client.pinned_pool[:pool], @client.primary_pool
  end

  def test_pinned_pool_is_local_to_thread
    threads = []
    30.times do |i|
      threads << Thread.new do
        if i % 2 == 0
          @coll.find_one({}, :read => :secondary_preferred)
          assert @client.secondary_pools.include? @client.pinned_pool[:pool]
        else
          @coll.find_one({}, :read => :primary_preferred)
          assert_equal @client.pinned_pool[:pool], @client.primary_pool
        end
      end
    end
    threads.each(&:join)
  end
end
