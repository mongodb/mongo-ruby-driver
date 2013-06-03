# Copyright (C) 2013 10gen Inc.
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

class ReplicaSetCountTest < Test::Unit::TestCase

  def setup
    ensure_cluster(:rs)
    @client = MongoReplicaSetClient.new(@rs.repl_set_seeds, :read => :primary_preferred)
    assert @client.primary_pool
    @primary = MongoClient.new(@client.primary_pool.host, @client.primary_pool.port)
    @db = @client.db(MONGO_TEST_DB)
    @db.drop_collection("test-sets")
    @coll = @db.collection("test-sets")
  end

  def teardown
    @client.close if @conn
  end

  def test_correct_count_after_insertion_reconnect
    @coll.insert({:a => 20}, :w => 3, :wtimeout => 10000)
    assert_equal 1, @coll.count

    # Kill the current master node
    @rs.primary.stop

    rescue_connection_failure do
      @coll.insert({:a => 30})
    end

    @coll.insert({:a => 40})
    assert_equal 3, @coll.count, "Second count failed"
  end

  def test_count_command_sent_to_primary
    @coll.insert({:a => 20}, :w => 3, :wtimeout => 10000)
    count_before = @primary['admin'].command({:serverStatus => 1})['opcounters']['command']
    assert_equal 1, @coll.count
    count_after = @primary['admin'].command({:serverStatus => 1})['opcounters']['command']
    assert_equal 2, count_after - count_before
  end

  def test_count_with_read
    @coll.insert({:a => 20}, :w => 3, :wtimeout => 10000)
    count_before = @primary['admin'].command({:serverStatus => 1})['opcounters']['command']
    assert_equal 1, @coll.count(:read => :secondary)
    assert_equal 1, @coll.find({}, :read => :secondary).count()
    count_after = @primary['admin'].command({:serverStatus => 1})['opcounters']['command']
    assert_equal 1, count_after - count_before
  end
end
