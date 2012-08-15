$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require './test/replica_sets/rs_test_helper'
require 'logger'

# Tags for members:
# 0 => {"dc" => "ny", "rack" => "a", "db" => "main"}
# 1 => {"dc" => "ny", "rack" => "b", "db" => "main"}
# 2 => {"dc" => "sf", "rack" => "a", "db" => "main"}

class ComplexReadPreferenceTest < Test::Unit::TestCase
  def setup
    ensure_rs

    # Insert data
    conn = Connection.new(@rs.host, @rs.primary[1])
    db = conn.db(MONGO_TEST_DB)
    coll = db.collection("test-sets")
    coll.save({:a => 20}, :safe => {:w => 2})
  end

  def test_primary_with_tags
    # Test specifying a tag set with default read preference of primary throws and error
    conn = make_connection({:tag_sets => {"rack" => "a"}})
    assert_raise_error MongoArgumentError, "Read preferecy :primary cannot be combined with tags" do
      conn.read_pool
    end
  end

  def test_tags
    return true if @rs.version < "2.1"

    assert_read_pool(:primary, {}, 0)
    assert_read_pool(:primary_preferred, {}, 0)
    assert_read_pool(:secondary, {}, [1,2])
    assert_read_pool(:secondary_preferred, {}, [1,2])

    # Test tag_sets are ignored on primary
    assert_read_pool(:primary_preferred,
      {"rack" => "b"}, 0)

    # Test single tag
    assert_read_pool(:secondary,
      {"rack" => "a"}, 2)
    assert_read_pool(:secondary,
      {"rack" => "b"}, 1)
    assert_read_pool(:secondary,
      {"db" => "main"}, [1, 2])

    # Test multiple tags
    assert_read_pool(:secondary,
      {"db" => "main", "rack" => "a"}, 2)
    assert_read_pool(:secondary,
      {"dc" => "ny", "rack" => "b", "db" => "main"}, 1)

    # Test multiple tags failing
    assert_fail_pool(:secondary,
      {"dc" => "ny", "rack" => "a"})
    assert_fail_pool(:secondary,
      {"dc" => "ny", "rack" => "b", "db" => "main", "xtra" => "?"})

    # Test symbol is converted to string for key
    assert_read_pool(:secondary,
      {:db => "main", "rack" => "b"}, 1)
    assert_read_pool(:secondary,
      {:db => "main", :rack => "b"}, 1)
    assert_read_pool(:secondary,
      {"db" => "main", :rack => "b"}, 1)

    # Test secondary_preferred
    assert_read_pool(:secondary_preferred,
      {"dc" => "ny"}, 1)
    assert_read_pool(:secondary_preferred,
      {"dc" => "sf"}, 2)
    assert_read_pool(:secondary_preferred,
      {"dc" => "china"}, 0)

    # Test secondary_preferred with no matching member
    assert_read_pool(:secondary_preferred,
      {"dc" => "bad"}, 0)
    assert_read_pool(:secondary_preferred,
      {"db" => "main", "dc" => "china"}, 0)
    assert_read_pool(:secondary_preferred,
      {"db" => "ny", "rack" => "a"}, 0)
  end

  def test_tag_sets
    return true if @rs.version < "2.1"

    # Test primary_preferred overrides any tags when primary is available
    assert_read_pool(:primary_preferred, [
      {"dc" => "sf"}
    ], 0)

    # Test first tag_set takes priority over the second
    assert_read_pool(:secondary, [
      {"dc" => "sf"},
      {"dc" => "ny"}
    ], 2)
    assert_read_pool(:secondary, [
      {"dc" => "ny"},
      {"dc" => "sf"}
    ], 1)
    assert_read_pool(:secondary_preferred, [
      {"dc" => "sf"},
      {"dc" => "ny"}
    ], 2)
    assert_read_pool(:secondary_preferred, [
      {"dc" => "ny"},
      {"dc" => "sf"}
    ], 1)

    # Test tags not matching any member throw an error
    assert_fail_pool(:secondary, [
      {"dc" => "ny", "rack" => "a"},
      {"dc" => "sf", "rack" => "b"},
    ])

    # Test bad tags get skipped over
    assert_read_pool(:secondary_preferred, [
      {"bad" => "tag"},
      {"dc" => "sf"}
    ], 2)

    # Test less selective tags
    assert_read_pool(:secondary, [
      {"dc" => "ny", "rack" => "b", "db" => "alt"},
      {"dc" => "ny", "rack" => "a"},
      {"dc" => "sf"}
    ], 2)
    assert_read_pool(:secondary_preferred, [
      {"dc" => "ny", "rack" => "b", "db" => "alt"},
      {"dc" => "ny", "rack" => "a"},
      {"dc" => "sf"}
    ], 2)
    assert_read_pool(:secondary_preferred, [
      {"dc" => "ny", "rack" => "a"},
      {"dc" => "sf", "rack" => "b"},
      {"db" => "main"}
    ], [1,2])

    # Test secondary preferred gives primary if no tags match
    assert_read_pool(:secondary_preferred, [
      {"dc" => "ny", "rack" => "a"},
      {"dc" => "sf", "rack" => "b"}
    ], 0)
    assert_read_pool(:secondary_preferred, [
      {"dc" => "ny", "rack" => "a"},
      {"dc" => "sf", "rack" => "b"},
      {"dc" => "ny", "rack" => "b"},
    ], 1)

    # Basic nearest test
    assert_read_pool(:nearest, [
      {"dc" => "ny", "rack" => "a"},
      {"dc" => "sf", "rack" => "b"},
      {"db" => "main"}
    ], [0,1,2])
  end

  def test_nearest
    # Test refresh happens on connection after interval has passed
    conn = make_connection(
      :read => :secondary_preferred,
      :refresh_mode => :sync,
      :refresh_interval => 1,
      :secondary_acceptable_latency_ms => 10
    )
    pools = conn.manager.pools

    # Connection should select node with 110 ping every time
    set_pings(pools, [100,110,130])
    sleep(2)

    assert conn.read_pool == pools[1]

    # Connection should select node with 100 ping every time
    set_pings(pools, [100,120,100])
    sleep(2)

    assert conn.read_pool == pools[2]
  end

  def test_tags_and_nearest
    return true if @rs.version < "2.1"

    # Test connection's read pool matches tags
    assert_read_pool(:secondary_preferred, {"dc" => "sf"}, 2, [100,110,130])

    # Test connection's read pool picks near pool (both match tags)
    assert_read_pool(:secondary_preferred, {"db" => "main"}, 1, [100,110,130])
    assert_read_pool(:secondary_preferred, {"db" => "main"}, 2, [100,130,110])
    assert_read_pool(:secondary_preferred, {"db" => "fake"}, 0, [100,130,110])
  end

  private

  def set_pings(pools, pings)
    pools.sort! { |a,b| a.port <=> b.port }
    pools.each_with_index do |pool, index|
      pool.stubs(:ping_time).returns(pings[index])
    end
  end

  def make_connection(opts = {})
    ReplSetConnection.new(build_seeds(3), opts)
  end

  def assert_read_pool(mode=:primary, tags=[], node_nums=[0], pings=[], latency=10)
    if pings.empty?
      conn = make_connection({:read => mode, :tag_sets => tags})
    else
      conn = make_connection({
        :read => mode,
        :tag_sets => tags,
        :refresh_mode => :sync,
        :refresh_interval => 1,
        :secondary_acceptable_latency_ms => latency
      })

      set_pings(conn.manager.pools, pings)
      sleep(2)
    end

    assert conn[MONGO_TEST_DB]['test-sets'].find_one

    target_ports = [*node_nums].collect {|num| @rs.ports[num]}

    assert target_ports.member?(conn.read_pool.port)
  end

  def assert_fail_pool(mode=:primary, tags={})
    assert_raise_error ConnectionFailure, "No replica set member available for query " +
      "with read preference matching mode #{mode} and tags matching #{tags}." do
      make_connection({:read => mode, :tag_sets => tags}).read_pool
    end
  end
end