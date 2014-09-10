# Copyright (C) 2009-2014 MongoDB, Inc.
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
require 'pp'
include Mongo

class ReadPreferenceTest < Test::Unit::TestCase
  TEST_DB = name.underscore
  TEST_COLL = name.underscore

  @@mo = Mongo::Orchestration::Service.new

  def await_replication(coll)
    coll.insert({'a' => 0}, :w => @n)
  end

  def setup
    @cluster = @@mo.configure({:orchestration => 'replica_sets', :request_content => {:id => 'replica_set_1', :preset => 'basic.json'} })
    @client = Mongo::MongoClient.from_uri(@cluster.object['mongodb_uri'])
    @client.drop_database(TEST_DB)
    @db = @client[TEST_DB]
    @coll = @db[TEST_COLL]
    await_replication(@coll)
    @primary = @cluster.primary
    @n = @cluster.object['members'].count
  end

  def teardown
    begin
      @client.drop_database(TEST_DB)
    rescue Mongo::ConnectionFailure => ex
    end
    @cluster.delete
  end

  # Scenario: Read Primary
  test 'Read Primary' do
    # Given a basic replica set
    # And a document written to all members
    @coll.insert({'a' => 1}, :w => @n)
    # When I read with read-preference PRIMARY
    # Then the read occurs on the primary
    reader = Mongo::MongoClient.from_uri(@cluster.object['mongodb_uri'], :read => :primary)
    assert_equal(@primary.object['uri'], reader.read_pool.address)
    assert_query_route(reader) do
      reader[TEST_DB][TEST_COLL].find_one({'a' => 1})
    end
    # When there is no primary
    @cluster.secondaries.first.stop
    @cluster.primary.stop
    # And I read with read-preference PRIMARY
    # Then the read fails
    ex = assert_raise Mongo::ConnectionFailure do
      reader[TEST_DB][TEST_COLL].find_one({'a' => 1})
    end
    assert_match(/No replica set member available for query with read preference matching/, ex.message)
  end

  # Scenario: Read Primary Preferred
  test 'Read Primary Preferred' do
    # Given a basic replica set
    # And a document written to all members
    @coll.insert({'a' => 1}, :w => @n)
    # When I read with read-preference PRIMARY_PREFERRED
    # Then the read occurs on the primary
    reader = Mongo::MongoClient.from_uri(@cluster.object['mongodb_uri'], :read => :primary_preferred)
    assert_equal(@primary.object['uri'], reader.read_pool.address)
    assert_query_route(reader) do
      reader[TEST_DB][TEST_COLL].find_one({'a' => 1})
    end
    # When there is no primary
    @cluster.secondaries.first.stop
    @cluster.primary.stop
    # And I read with read-preference PRIMARY_PREFERRED
    # Then the read succeeds
    assert(reader[TEST_DB][TEST_COLL].find_one({'a' => 1}))
  end

  # Scenario: Read Secondary
  test 'Read Secondary' do
    # Given a basic replica set
    # And a document written to all members
    @coll.insert({'a' => 1}, :w => @n)
    # When I read with read-preference SECONDARY
    # Then the read occurs on the secondary
    reader = Mongo::MongoClient.from_uri(@cluster.object['mongodb_uri'], :read => :secondary)
    assert_not_equal(@primary.object['uri'], reader.read_pool.address)
    assert_query_route(reader) do
      reader[TEST_DB][TEST_COLL].find_one({'a' => 1})
    end
    # Note: With a basic replica set, it is not possible to test the following, which would be possible with a replica set composed of two data members plus an arbiter.                                                                                                                                                                  When there are no secondaries
    # When there are no secondaries
    # And I read with read-preference SECONDARY
    # Then the read fails
  end

  # Scenario: Read Secondary Preferred
  test 'Read Secondary Preferred' do
    # Given a basic replica set
    # And a document written to all members
    @coll.insert({'a' => 1}, :w => @n)
    # When I read with read-preference SECONDARY_PREFERRED
    # Then the read occurs on the secondary
    reader = Mongo::MongoClient.from_uri(@cluster.object['mongodb_uri'], :read => :secondary_preferred)
    assert_not_equal(@primary.object['uri'], reader.read_pool.address)
    assert_query_route(reader) do
      reader[TEST_DB][TEST_COLL].find_one({'a' => 1})
    end
    # Note: With a basic replica set, it is not possible to test the following, which would be possible with a replica set composed of two data members plus an arbiter.                                                                                                                                                                  When there are no secondaries
    # When there are no secondaries
    # And I read with read-preference SECONDARY
    # Then the read succeeds
  end

  private

  def query_count(connection)
    connection['admin'].command({:serverStatus => 1})['opcounters']['query']
  end

  def assert_query_route(reader)
    direct_client = Mongo::MongoClient.new(reader.read_pool.host, reader.read_pool.port)
    queries_before = query_count(direct_client)
    assert_nothing_raised do
      yield
    end
    queries_after = query_count(direct_client)
    assert_equal(1, queries_after - queries_before, 'expect query count to increment by one')
  end
end
