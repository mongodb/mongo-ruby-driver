require 'test_helper'

class ReplicaSetCursorTest < Test::Unit::TestCase

  def setup
    ensure_cluster(:rs)
  end

  def test_cursors_get_closed
    setup_client
    assert_cursor_count
  end

  #def test_cursors_get_closed_secondary
  #  setup_client(:secondary)
  #  assert_cursor_count
  #end

  private

  def setup_client(read=:primary)
    # Setup ReplicaSet Connection
    @client = MongoReplicaSetClient.new(
        @rs.repl_set_seeds,
      :read => read
    )

    @db = @client.db(MONGO_TEST_DB)
    @db.drop_collection("cursor_tests")
    @coll = @db.collection("cursor_tests")

    @coll.insert({:a => 1}, :w => 2)
    @coll.insert({:b => 2}, :w => 2)
    @coll.insert({:c => 3}, :w => 2)

    # Pin reader
    @coll.find_one

    # Setup Direct Connections
    @primary = Mongo::MongoClient.new(*@client.manager.primary)
    @read = Mongo::MongoClient.new(*@client.manager.read)
  end

  def cursor_count(client)
    client['cursor_tests'].command({:cursorInfo => 1})['totalOpen']
  end

  def query_count(client)
    client['admin'].command({:serverStatus => 1})['opcounters']['query']
  end

  def assert_cursor_count
    before_primary = cursor_count(@primary)
    before_read = cursor_count(@read)
    before_query = query_count(@read)

    @coll.find.limit(2).to_a
    sleep(1)

    after_primary = cursor_count(@primary)
    after_read = cursor_count(@read)
    after_query = query_count(@read)

    assert_equal before_primary, after_primary
    assert_equal before_read, after_read
    assert_equal 1, after_query - before_query
  end

end

