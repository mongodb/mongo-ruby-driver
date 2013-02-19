require 'test_helper'

class ReplicaSetCursorTest < Test::Unit::TestCase

  def setup
    ensure_cluster(:rs)
  end

  def test_get_more_primary
    setup_client(:primary)
    cursor_get_more_test(:primary)
  end

  def test_get_more_secondary
    setup_client(:secondary)
    cursor_get_more_test(:secondary)
  end

  def test_close_primary
    setup_client(:primary)
    cursor_close_test(:primary)
  end

  def test_close_secondary
    setup_client(:secondary)
    cursor_close_test(:secondary)
  end

  def test_cursors_get_closed
    setup_client
    assert_cursor_count
  end

  def test_cursors_get_closed_secondary
    setup_client(:secondary)
    assert_cursor_count(:secondary)
  end

  def test_cursors_get_closed_secondary_query
    setup_client(:primary)
    assert_cursor_count(:secondary)
  end

  private

  def setup_client(read=:primary)
    route_read ||= read
    # Setup ReplicaSet Connection
    @client = MongoReplicaSetClient.new(@rs.repl_set_seeds, :read => read)

    @db = @client.db(MONGO_TEST_DB)
    @db.drop_collection("cursor_tests")
    @coll = @db.collection("cursor_tests")

    @coll.insert({:a => 1}, :w => 3)
    @coll.insert({:b => 2}, :w => 3)
    @coll.insert({:c => 3}, :w => 3)

    # Pin reader
    @coll.find_one

    # Setup Direct Connections
    @primary = Mongo::MongoClient.new(*@client.manager.primary)
  end

  def cursor_count(client)
    client['cursor_tests'].command({:cursorInfo => 1})['totalOpen']
  end

  def query_count(client)
    client['admin'].command({:serverStatus => 1})['opcounters']['query']
  end

  def set_read_client_and_tag(read)
    read_opts = {:read => read}
    @tag = (0...3).map{|i|i.to_s}.detect do |tag|
      begin
        read_opts[:tag_sets] = [{:node => tag}] unless read == :primary
        cursor = @coll.find({}, read_opts)
        cursor.next
        pool = cursor.instance_variable_get(:@pool)
        cursor.close
        @read = Mongo::MongoClient.new(pool.host, pool.port)
        tag
      rescue Mongo::ConnectionFailure
        false
      end
    end
  end

  def assert_cursor_count(read=:primary)
    set_read_client_and_tag(read)

    before_primary_cursor = cursor_count(@primary)
    before_read_cursor = cursor_count(@read)
    before_read_query = query_count(@read)

    read_opts = {:read => read}
    read_opts[:tag_sets] = [{:node => @tag}] unless read == :primary
    @coll.find({}, read_opts).limit(2).to_a

    after_primary_cursor = cursor_count(@primary)
    after_read_cursor = cursor_count(@read)
    after_read_query = query_count(@read)

    assert_equal before_primary_cursor, after_primary_cursor
    assert_equal before_read_cursor, after_read_cursor
    assert_equal 1, after_read_query - before_read_query
  end

  def insert_docs
    102.times do |i|
      @coll.insert({:i =>i}, :w => 3)
    end
  end

  # batch from send_initial_query is 101 documents
  def cursor_get_more_test(read=:primary)
    insert_docs
    10.times do
      cursor = @coll.find({}, :read => read)
      cursor.next
      port = cursor.instance_variable_get(:@pool).port
      assert cursor.alive?
      while cursor.has_next?
        cursor.next
        assert_equal port, cursor.instance_variable_get(:@pool).port
      end
      assert !cursor.alive?
      cursor.close #cursor is already closed
    end
  end

  # batch from get_more can be huge, so close after send_initial_query
  def cursor_close_test(read=:primary)
    insert_docs
    10.times do
      cursor = @coll.find({}, :read => read)
      cursor.next
      assert cursor.instance_variable_get(:@pool)
      assert cursor.alive?
      cursor.close
    end
  end
end
