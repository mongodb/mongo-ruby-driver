require 'test_helper'
# TODO: create more tests for one primrary and multiple secondaries
class ReplicaSetCursorTest < Test::Unit::TestCase

  def setup
    ensure_cluster(:rs)
  end
# set up default reads to be primrary
  def test_cursors_get_closed
    setup_client
    assert_query_route
    assert_cursors_closed
  end
# set up default reads to be secondary
  def test_cursors_get_closed_secondary
    setup_client(:secondary)
    assert_query_route
    assert_cursors_closed
  end
# set up default reads to be primrary but override read pref to secondary on query
  def test_cursors_get_closed_secondary_query
    setup_client(:primary, :secondary)
    assert_query_route
    assert_cursors_closed
  end

  private

  def setup_client(read=:primary, route_read=nil)
    route_read ||= read
    # setup ReplicaSet Connection
    @client = MongoReplicaSetClient.new(@rs.repl_set_seeds, :read => read)
    @db = @client.db(MONGO_TEST_DB)
    @coll = @db.collection("cursor_tests")
    @coll.drop
    # setup up direct connections (primary not used now, but could be later)
    @primary = Mongo::MongoClient.new(*@client.manager.primary)
    # slave_ok => true is necessary so that we can read system.profile
    @read = Mongo::MongoClient.new(*@client.manager.read_pool(route_read).host_port << {:slave_ok => true})
    # batch size is assumed to be 101, so 102 would trigger a get_more
    @object_id = BSON::ObjectId.new
    102.times do |i|
      @coll.insert({ "x" => @object_id }, {:w => 2})
    end
    # set profiling level to 2 on client and member to which the query will be routed
    @client.db(MONGO_TEST_DB).profiling_level = :all
    @read.db(MONGO_TEST_DB).profiling_level = :all
    # do a query using client
    @cursor = @coll.find({"x" => @object_id }, :read => route_read)
    # .next actually triggers the query on the member
    @cursor.next
    # on client and read-routed member set profiling level to 0
    @client.db(MONGO_TEST_DB).profiling_level = :off
    @read.db(MONGO_TEST_DB).profiling_level = :off
  end

  def assert_query_route
    read_results = @read.db(MONGO_TEST_DB).collection('system.profile').find()
    # do a query on system.profile to see if it was used for the query
    profiled_queries = @read.db(MONGO_TEST_DB).collection('system.profile').find({
      'ns' => "#{MONGO_TEST_DB}.cursor_tests", "query.x" => @object_id })
    # confirm that the query has been done on the member to which the read should have been routed
    puts "profiled queries: #{profiled_queries.count}"
    assert_equal 1, profiled_queries.count
  end

  def assert_cursors_closed
    cursor_id = @cursor.cursor_id
    cursor_clone = @cursor.clone
    assert_equal cursor_id, cursor_clone.cursor_id
    # first batch was assumed to be 101, so iterate through the first batch.
    # Note that we did one .next on the cursor above
    100.times { @cursor.next }
    # kill the cursor
    @cursor.close
    # do a get more on the new cursor
    # assert that the server returns an error "cursor_id 'XXXX' not valid at server"
    # NOTE: assumes that the cursor is not exhausted.  Check batch size if this fails.
    assert_raise Mongo::OperationFailure do
      cursor_clone.next
    end
  end
end