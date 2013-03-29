require 'test_helper'
include Mongo

class Cursor
  public :construct_query_spec
end

class BasicTest < Test::Unit::TestCase

  def setup
    ensure_cluster(:sc)
    @document = { "name" => "test_user" }
    @seeds = @sc.mongos_seeds
  end

  # TODO member.primary? ==> true
  def test_connect
    @client = MongoShardedClient.new(@seeds)
    assert @client.connected?
    assert_equal(@seeds.size, @client.seeds.size)
    probe(@seeds.size)
    @client.close
  end

  def test_connect_from_standard_client
    mongos = @seeds.first
    @client = MongoClient.new(*mongos.split(':'))
    assert @client.connected?
    assert @client.mongos?
    @client.close
  end

  def test_read_from_client
    host, port = @seeds.first.split(':')
    tags = [{:dc => "mongolia"}]
    @client = MongoClient.new(host, port, {:read => :secondary, :tag_sets => tags})
    assert @client.connected?
    cursor = Cursor.new(@client[MONGO_TEST_DB]['whatever'], {})
    assert_equal cursor.construct_query_spec['$readPreference'], {:mode => :secondary, :tags => tags}
  end

  def test_find_one_with_read_secondary
    @client = MongoShardedClient.new(@seeds, { :read => :secondary })
    @client[MONGO_TEST_DB]["users"].insert([ @document ])
    assert_equal @client[MONGO_TEST_DB]['users'].find_one["name"], "test_user"
  end

  def test_find_one_with_read_secondary_preferred
    @client = MongoShardedClient.new(@seeds, { :read => :secondary_preferred })
    @client[MONGO_TEST_DB]["users"].insert([ @document ])
    assert_equal @client[MONGO_TEST_DB]['users'].find_one["name"], "test_user"
  end

  def test_find_one_with_read_primary
    @client = MongoShardedClient.new(@seeds, { :read => :primary })
    @client[MONGO_TEST_DB]["users"].insert([ @document ])
    assert_equal @client[MONGO_TEST_DB]['users'].find_one["name"], "test_user"
  end

  def test_find_one_with_read_primary_preferred
    @client = MongoShardedClient.new(@seeds, { :read => :primary_preferred })
    @client[MONGO_TEST_DB]["users"].insert([ @document ])
    assert_equal @client[MONGO_TEST_DB]['users'].find_one["name"], "test_user"
  end

  def test_read_from_sharded_client
    tags = [{:dc => "mongolia"}]
    @client = MongoShardedClient.new(@seeds, {:read => :secondary, :tag_sets => tags})
    assert @client.connected?
    cursor = Cursor.new(@client[MONGO_TEST_DB]['whatever'], {})
    assert_equal cursor.construct_query_spec['$readPreference'], {:mode => :secondary, :tags => tags}
  end

  def test_hard_refresh
    @client = MongoShardedClient.new(@seeds)
    assert @client.connected?
    @client.hard_refresh!
    assert @client.connected?
    @client.close
  end

  def test_reconnect
    @client = MongoShardedClient.new(@seeds)
    assert @client.connected?
    router = @sc.servers(:routers).first
    router.stop
    probe(@seeds.size)
    assert @client.connected?
    @client.close
  end

  def test_mongos_failover
    @client = MongoShardedClient.new(@seeds, :refresh_interval => 5, :refresh_mode => :sync)
    assert @client.connected?
    # do a find to pin a pool
    @client['MONGO_TEST_DB']['test'].find_one
    original_primary = @client.manager.primary
    # stop the pinned member
    @sc.member_by_name("#{original_primary[0]}:#{original_primary[1]}").stop
    # assert that the client fails over to the next available mongos
    assert_nothing_raised do
      @client['MONGO_TEST_DB']['test'].find_one
    end

    assert_not_equal original_primary, @client.manager.primary
    assert @client.connected?
    @client.close
  end

  def test_all_down
    @client = MongoShardedClient.new(@seeds)
    assert @client.connected?
    @sc.servers(:routers).each{|router| router.stop}
    assert_raises Mongo::ConnectionFailure do
      probe(@seeds.size)
    end
    assert_false @client.connected?
    @client.close
  end

  def test_cycle
    @client = MongoShardedClient.new(@seeds)
    assert @client.connected?
    routers = @sc.servers(:routers)
    while routers.size > 0 do
      rescue_connection_failure do
        probe(@seeds.size)
      end
      probe(@seeds.size)
      router = routers.detect{|r| r.port == @client.manager.primary.last}
      routers.delete(router)
      router.stop
    end
    assert_raises Mongo::ConnectionFailure do
      probe(@seeds.size)
    end
    assert_false @client.connected?
    routers = @sc.servers(:routers).reverse
    routers.each do |r|
      r.start
      @client.hard_refresh!
      rescue_connection_failure do
        probe(@seeds.size)
      end
      probe(@seeds.size)
    end
    @client.close
  end

  private

  def probe(size)
    assert_equal(size, @client['config']['mongos'].find.to_a.size)
  end
end
