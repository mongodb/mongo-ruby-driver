# Copyright (C) 2009-2013 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
    kill_cursor_test(:primary)
  end

  def test_close_secondary
    setup_client(:secondary)
    kill_cursor_test(:secondary)
  end

  def test_cursors_get_closed
    setup_client
    assert_cursors_on_members
  end

  def test_cursors_get_closed_secondary
    setup_client(:secondary)
    assert_cursors_on_members(:secondary)
  end

  def test_cursors_get_closed_secondary_query
    setup_client(:primary)
    assert_cursors_on_members(:secondary)
  end

  def test_intervening_query_secondary
    setup_client(:primary)
    refresh_while_iterating(:secondary)
  end

  private

  def setup_client(read=:primary)
    route_read ||= read
    # Setup ReplicaSet Connection
    @client = MongoReplicaSetClient.new(@rs.repl_set_seeds, :read => read)
    authenticate_client(@client)

    @db = @client.db(TEST_DB)
    @db.drop_collection("cursor_tests")
    @coll = @db.collection("cursor_tests")
    insert_docs

    # Setup Direct Connections
    @primary = Mongo::MongoClient.new(*@client.manager.primary)
    authenticate_client(@primary)
  end

  def insert_docs
    @n_docs = 102 # batch size is 101
    @n_docs.times do |i|
      @coll.insert({ "x" => i }, :w => 3)
    end
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
        @read = Mongo::MongoClient.new(pool.host, pool.port, :slave_ok => true)
        authenticate_client(@read)
        tag
      rescue Mongo::ConnectionFailure
        false
      end
    end
  end

  def route_query(read)
    read_opts = {:read => read}
    read_opts[:tag_sets] = [{:node => @tag}] unless read == :primary
    object_id = BSON::ObjectId.new
    read_opts[:comment] = object_id

    # set profiling level to 2 on client and member to which the query will be routed
    @client.db(TEST_DB).profiling_level = :all
    @client.secondaries.each do |node|
      node = Mongo::MongoClient.new(node[0], node[1], :slave_ok => true)
      authenticate_client(node)
      node.db(TEST_DB).profiling_level = :all
    end

    @cursor = @coll.find({}, read_opts)
    @cursor.next

    # on client and other members set profiling level to 0
    @client.db(TEST_DB).profiling_level = :off
    @client.secondaries.each do |node|
      node = Mongo::MongoClient.new(node[0], node[1], :slave_ok => true)
      authenticate_client(node)
      node.db(TEST_DB).profiling_level = :off
    end
    # do a query on system.profile of the reader to see if it was used for the query
    profiled_queries = @read.db(TEST_DB).collection('system.profile').find({
      'ns' => "#{TEST_DB}.cursor_tests", "query.$comment" => object_id })

    assert_equal 1, profiled_queries.count
  end

  # batch from send_initial_query is 101 documents
  # check that you get n_docs back from the query, with the same port
  def cursor_get_more_test(read=:primary)
    return if subject_to_server_4754?(@client)
    set_read_client_and_tag(read)
    10.times do
      # assert that the query went to the correct member
      route_query(read)
      docs_count = 1
      port = @cursor.instance_variable_get(:@pool).port
      assert @cursor.alive?
      while @cursor.has_next?
        docs_count += 1
        @cursor.next
        assert_equal port, @cursor.instance_variable_get(:@pool).port
      end
      assert !@cursor.alive?
      assert_equal @n_docs, docs_count
      @cursor.close #cursor is already closed
    end
  end

  # batch from get_more can be huge, so close after send_initial_query
  def kill_cursor_test(read=:primary)
    return if subject_to_server_4754?(@client)
    set_read_client_and_tag(read)
    10.times do
      # assert that the query went to the correct member
      route_query(read)
      cursor_id = @cursor.cursor_id
      cursor_clone = @cursor.clone
      assert_equal cursor_id, cursor_clone.cursor_id
      assert @cursor.instance_variable_get(:@pool)
      # .next was called once already and leave one for get more
      (@n_docs-2).times { @cursor.next }
      @cursor.close
      # an exception confirms the cursor has indeed been closed
      assert_raise Mongo::OperationFailure do
        cursor_clone.next
      end
    end
  end

  def assert_cursors_on_members(read=:primary)
    return if subject_to_server_4754?(@client)
    set_read_client_and_tag(read)
    # assert that the query went to the correct member
    route_query(read)
    cursor_id = @cursor.cursor_id
    cursor_clone = @cursor.clone
    assert_equal cursor_id, cursor_clone.cursor_id
    assert @cursor.instance_variable_get(:@pool)
    port = @cursor.instance_variable_get(:@pool).port
    while @cursor.has_next?
      @cursor.next
      assert_equal port, @cursor.instance_variable_get(:@pool).port
    end
    # an exception confirms the cursor has indeed been closed after query
    assert_raise Mongo::OperationFailure do
      cursor_clone.next
    end
  end

  def refresh_while_iterating(read)
    set_read_client_and_tag(read)

    read_opts = {:read => read}
    read_opts[:tag_sets] = [{:node => @tag}]
    read_opts[:batch_size] = 2
    cursor = @coll.find({}, read_opts)

    2.times { cursor.next }
    port = cursor.instance_variable_get(:@pool).port
    host = cursor.instance_variable_get(:@pool).host
    # Refresh connection
    @client.refresh
    assert_nothing_raised do
      cursor.next
    end

    assert_equal port, cursor.instance_variable_get(:@pool).port
    assert_equal host, cursor.instance_variable_get(:@pool).host
  end
end