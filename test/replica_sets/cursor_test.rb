$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require './test/replica_sets/rs_test_helper'

class ReplicaSetCursorTest < Test::Unit::TestCase
  def setup
    ensure_rs

    # Setup Direct Connections
    @primary = Mongo::Connection.new("#{@rs.host}", @rs.ports[0])
    @secondary = Mongo::Connection.new("#{@rs.host}", @rs.ports[1])
  end

  def setup_connection(read=:primary)
    # Setup ReplicaSet Connection
    @replconn = Mongo::ReplSetConnection.new(
      build_seeds(2),
      :read => read
    )
  end

  def setup_collection
    @db = @replconn.db(MONGO_TEST_DB)
    @db.drop_collection("cursor_tests")
    @coll = @db.collection("cursor_tests")

    @coll.insert({:a => 1})
    @coll.insert({:b => 2})
    @coll.insert({:c => 3})
  end

  def cursor_count(connection)
    connection['cursor_tests'].command({:cursorInfo => 1})['totalOpen']
  end

  def assert_cursor_count
    before_primary   = cursor_count(@primary)
    before_secondary = cursor_count(@secondary)

    @coll.find.limit(2).to_a

    after_primary   = cursor_count(@primary)
    after_secondary = cursor_count(@secondary)

    assert_equal before_primary, after_primary
    assert_equal before_secondary, after_secondary
  end

  def test_cursors_get_closed
    setup_connection
    setup_collection
    assert_cursor_count
  end

  def test_cursors_get_closed_secondary
    setup_connection(:secondary)
    setup_collection
    assert_cursor_count
  end

  def test_cursors_get_closed_secondary_only
    setup_connection(:secondary_only)
    setup_collection
    assert_cursor_count
  end
end

