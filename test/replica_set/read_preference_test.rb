require 'test_helper'

class ReadPreferenceTest < Test::Unit::TestCase

  def setup
    ensure_cluster(:rs, :replicas => 2, :arbiters => 0)

    # Insert data
    primary = @rs.primary
    conn = Connection.new(primary.host, primary.port)
    db = conn.db(MONGO_TEST_DB)
    coll = db.collection("test-sets")
    coll.save({:a => 20}, {:w => 2})
  end

  def test_read_primary
    conn = make_connection
    rescue_connection_failure do
      assert conn.read_primary?
      assert conn.primary?
    end

    conn = make_connection(:primary_preferred)
    rescue_connection_failure do
      assert conn.read_primary?
      assert conn.primary?
    end

    conn = make_connection(:secondary)
    rescue_connection_failure do
      assert !conn.read_primary?
      assert !conn.primary?
    end

    conn = make_connection(:secondary_preferred)
    rescue_connection_failure do
      assert !conn.read_primary?
      assert !conn.primary?
    end
  end

  def test_connection_pools
    conn = make_connection
    assert conn.primary_pool, "No primary pool!"
    assert conn.read_pool, "No read pool!"
    assert conn.primary_pool.port == conn.read_pool.port,
      "Primary port and read port are not the same!"

    conn = make_connection(:primary_preferred)
    assert conn.primary_pool, "No primary pool!"
    assert conn.read_pool, "No read pool!"
    assert conn.primary_pool.port == conn.read_pool.port,
      "Primary port and read port are not the same!"

    conn = make_connection(:secondary)
    assert conn.primary_pool, "No primary pool!"
    assert conn.read_pool, "No read pool!"
    assert conn.primary_pool.port != conn.read_pool.port,
      "Primary port and read port are the same!"

    conn = make_connection(:secondary_preferred)
    assert conn.primary_pool, "No primary pool!"
    assert conn.read_pool, "No read pool!"
    assert conn.primary_pool.port != conn.read_pool.port,
      "Primary port and read port are the same!"
  end

  def test_read_routing
    prepare_routing_test

    # Test that reads are going to the right members
    assert_query_route(@primary, @primary_direct)
    assert_query_route(@primary_preferred, @primary_direct)
    assert_query_route(@secondary, @secondary_direct)
    assert_query_route(@secondary_preferred, @secondary_direct)
  end

  def test_read_routing_with_primary_down
    prepare_routing_test

    # Test that reads are going to the right members
    assert_query_route(@primary, @primary_direct)
    assert_query_route(@primary_preferred, @primary_direct)
    assert_query_route(@secondary, @secondary_direct)
    assert_query_route(@secondary_preferred, @secondary_direct)

    # Kill the primary so only a single secondary exists
    @rs.primary.kill

    # Test that reads are going to the right members
    assert_raise_error ConnectionFailure do
      @primary[MONGO_TEST_DB]['test-sets'].find_one
    end
    assert_query_route(@primary_preferred, @secondary_direct)
    assert_query_route(@secondary, @secondary_direct)
    assert_query_route(@secondary_preferred, @secondary_direct)

    # Restore set
    @rs.restart
    sleep(1)
    @repl_cons.each { |con| con.refresh }
    sleep(1)
    @primary_direct = Connection.new(
      @rs.config['host'],
      @primary.read_pool.port
    )

    # Test that reads are going to the right members
    assert_query_route(@primary, @primary_direct)
    assert_query_route(@primary_preferred, @primary_direct)
    assert_query_route(@secondary, @secondary_direct)
    assert_query_route(@secondary_preferred, @secondary_direct)
  end

  def test_read_routing_with_secondary_down
    prepare_routing_test

    # Test that reads are going to the right members
    assert_query_route(@primary, @primary_direct)
    assert_query_route(@primary_preferred, @primary_direct)
    assert_query_route(@secondary, @secondary_direct)
    assert_query_route(@secondary_preferred, @secondary_direct)

    # Kill the secondary so that only primary exists
    @rs.secondaries.first.kill

    # Test that reads are going to the right members
    assert_query_route(@primary, @primary_direct)
    assert_query_route(@primary_preferred, @primary_direct)
    assert_raise_error ConnectionFailure do
      @secondary[MONGO_TEST_DB]['test-sets'].find_one
    end
    assert_query_route(@secondary_preferred, @primary_direct)

    # Restore set
    @rs.restart
    sleep(1)
    @repl_cons.each { |con| con.refresh }
    sleep(1)
    @secondary_direct = Connection.new(
      @rs.config['host'],
      @secondary.read_pool.port,
      :slave_ok => true
    )

    # Test that reads are going to the right members
    assert_query_route(@primary, @primary_direct)
    assert_query_route(@primary_preferred, @primary_direct)
    assert_query_route(@secondary, @secondary_direct)
    assert_query_route(@secondary_preferred, @secondary_direct)
  end

  def test_write_lots_of_data
    @conn = make_connection(:secondary_preferred)
    @db = @conn[MONGO_TEST_DB]
    @coll = @db.collection("test-sets", {:w => 2})

    6000.times do |n|
      @coll.save({:a => n})
    end

    cursor = @coll.find()
    cursor.next
    cursor.close
  end

  private

  def prepare_routing_test
    # Setup replica set connections
    @primary = make_connection(:primary)
    @primary_preferred = make_connection(:primary_preferred)
    @secondary = make_connection(:secondary)
    @secondary_preferred = make_connection(:secondary_preferred)
    @repl_cons = [@primary, @primary_preferred, @secondary, @secondary_preferred]

    # Setup direct connections
    @primary_direct = Connection.new(@rs.config['host'], @primary.read_pool.port)
    @secondary_direct = Connection.new(@rs.config['host'], @secondary.read_pool.port, :slave_ok => true)
  end

  def make_connection(mode = :primary, opts = {})
    opts.merge!({:read => mode})
    MongoReplicaSetClient.new(@rs.repl_set_seeds, opts)
  end

  def query_count(connection)
    connection['admin'].command({:serverStatus => 1})['opcounters']['query']
  end

  def assert_query_route(test_connection, expected_target)
    #puts "#{test_connection.read_pool.port} #{expected_target.read_pool.port}"
    queries_before = query_count(expected_target)
    assert_nothing_raised do
      test_connection['MONGO_TEST_DB']['test-sets'].find_one
    end
    queries_after = query_count(expected_target)
    assert_equal 1, queries_after - queries_before
  end
end