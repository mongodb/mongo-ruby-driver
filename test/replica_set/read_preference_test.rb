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

class ReadPreferenceTest < Test::Unit::TestCase

  def setup
    ensure_cluster(:rs, :replicas => 2, :arbiters => 0)
    client = make_connection
    db = client.db(TEST_DB)
    coll = db.collection('test-sets')
    coll.save({:a => 20}, {:w => 2})
  end

  def test_read_primary
    client = make_connection
    rescue_connection_failure do
      assert client.read_primary?
      assert client.primary?
    end

    client = make_connection(:primary_preferred)
    rescue_connection_failure do
      assert client.read_primary?
      assert client.primary?
    end

    client = make_connection(:secondary)
    rescue_connection_failure do
      assert !client.read_primary?
      assert !client.primary?
    end

    client = make_connection(:secondary_preferred)
    rescue_connection_failure do
      assert !client.read_primary?
      assert !client.primary?
    end
  end

  def test_connection_pools
    client = make_connection
    assert client.primary_pool, "No primary pool!"
    assert client.read_pool, "No read pool!"
    assert client.primary_pool.port == client.read_pool.port,
      "Primary port and read port are not the same!"


    client = make_connection(:primary_preferred)
    assert client.primary_pool, "No primary pool!"
    assert client.read_pool, "No read pool!"
    assert client.primary_pool.port == client.read_pool.port,
      "Primary port and read port are not the same!"

    client = make_connection(:secondary)
    assert client.primary_pool, "No primary pool!"
    assert client.read_pool, "No read pool!"
    assert client.primary_pool.port != client.read_pool.port,
      "Primary port and read port are the same!"

    client = make_connection(:secondary_preferred)
    assert client.primary_pool, "No primary pool!"
    assert client.read_pool, "No read pool!"
    assert client.primary_pool.port != client.read_pool.port,
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
    sleep(2)
    # Test that reads are going to the right members
    assert_raise_error ConnectionFailure do
      @primary[TEST_DB]['test-sets'].find_one
    end
    assert_query_route(@primary_preferred, @secondary_direct)
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
    @rs.secondaries.first.kill

    assert_query_route(@primary_preferred, @primary_direct)

    primary_read_only = false
    until primary_read_only
      config = @primary_direct['admin'].command(:isMaster => 1)
      primary_read_only = config["secondary"]
      sleep(1)
    end
    assert_query_route(@secondary_preferred, @primary_direct)

    # Restore set
    @rs.restart
    sleep(1)
    @repl_cons.each { |con| con.refresh }
    sleep(1)

    true_secondary = [@primary_direct, @secondary_direct].find do |client|
      client['admin'].command(:isMaster => 1)['secondary']
    end

    true_primary = [@primary_direct, @secondary_direct].find do |client|
      client['admin'].command(:isMaster => 1)['primary']
    end

    @secondary_direct, @primary_direct = true_secondary, true_primary

    # Test that reads are going to the right members
    assert_query_route(@primary, @primary_direct)
    assert_query_route(@primary_preferred, @primary_direct)
    assert_query_route(@secondary, @secondary_direct)
    assert_query_route(@secondary_preferred, @primary_direct) # primary pool is pinned
  end

  def test_write_lots_of_data
    client = make_connection(:secondary_preferred)
    db = client[TEST_DB]
    coll = db.collection("test-sets", {:w => 2})

    6000.times do |n|
      coll.save({:a => n})
    end

    cursor = coll.find()
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
    @primary_direct = MongoClient.new(@rs.config['host'], @primary.read_pool.port, :slave_ok => true)
    authenticate_client(@primary_direct)
    @secondary_direct = MongoClient.new(@rs.config['host'], @secondary.read_pool.port, :slave_ok => true)
    authenticate_client(@secondary_direct)
  end

  def make_connection(mode = :primary, opts = {})
    opts.merge!({:read => mode})
    client = MongoReplicaSetClient.new(@rs.repl_set_seeds, opts)
    authenticate_client(client)
  end

  def query_count(connection)
    connection['admin'].command({:serverStatus => 1})['opcounters']['query']
  end

  def assert_query_route(test_connection, expected_target)
    authenticate_client(test_connection)
    authenticate_client(expected_target)
    queries_before = query_count(expected_target)
    assert_nothing_raised do
      test_connection[TEST_DB]['test-sets'].find_one
    end
    queries_after = query_count(expected_target)
    assert_equal 1, queries_after - queries_before
  end
end