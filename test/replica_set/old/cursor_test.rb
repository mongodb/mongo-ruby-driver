$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require './test/replica_sets/rs_test_helper'

class ReplicaSetCursorTest < Test::Unit::TestCase
  def setup
    ensure_rs
  end

  def test_cursors_get_closed
    setup_client
    assert_cursor_count
  end

  def test_cursors_get_closed_secondary
    setup_client(:secondary)
    assert_cursor_count
  end

  private

  def setup_client(read=:primary)
    # Setup ReplicaSet Client
    @client = Mongo::ReplSetClient.new(
      build_seeds(3),
      :read => read
    )

    @db = @client.db(MONGO_TEST_DB)
    @db.drop_collection("cursor_tests")
    @coll = @db.collection("cursor_tests")

    @coll.insert({:a => 1}, :safe => true, :w => 3)
    @coll.insert({:b => 2}, :safe => true, :w => 3)
    @coll.insert({:c => 3}, :safe => true, :w => 3)

    # Pin reader
    @coll.find_one

    # Setup Direct Connections
    @primary = Mongo::Client.new(*@client.manager.primary)
    @read = Mongo::Client.new(*@client.manager.read)
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

