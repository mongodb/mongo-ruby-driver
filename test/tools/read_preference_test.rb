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
    # When I read with read-preference ‘primary’
    # Then the read occurs on the primary
    reader = Mongo::MongoClient.from_uri(@cluster.object['mongodb_uri'], :read => :primary)
    assert_query_route(direct_client(@primary.object['uri'])) do
      reader[TEST_DB][TEST_COLL].find_one({'a' => 1})
    end
    # When there is no primary
    @cluster.secondaries.first.stop
    @cluster.primary.stop
    # And I read with read-preference ‘primary’
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
    # When I read with read-preference ‘primaryPreferred’
    # Then the read occurs on the primary
    reader = Mongo::MongoClient.from_uri(@cluster.object['mongodb_uri'], :read => :primary_preferred)
    assert_query_route(direct_client(@primary.object['uri'])) do
      reader[TEST_DB][TEST_COLL].find_one({'a' => 1})
    end
    # When there is no primary
    @cluster.secondaries.first.stop
    @cluster.primary.stop
    # And I read with read-preference ‘primaryPreferred’
    # Then the read succeeds
    assert(reader[TEST_DB][TEST_COLL].find_one({'a' => 1}))
  end

  # Scenario: Read Secondary
  test 'Read Secondary' do
    # Given a basic replica set
    # And a document written to all members
    @coll.insert({'a' => 1}, :w => @n)
    # When I read with read-preference ‘secondary’
    # Then the read occurs on the secondary
  end

  # Scenario: Read Secondary Preferred
  test 'Read Secondary Preferred' do
    # Given a basic replica set
    # And a document written to all members
    @coll.insert({'a' => 1}, :w => @n)
    # When I read with read-preference ‘secondaryPreferred’
    # Then the read occurs on the secondary
  end

  private

  def direct_client(host_port)
    host, port = host_port.split(':', -1)
    Mongo::MongoClient.new(host, port.to_i)
  end

  def query_count(connection)
    connection['admin'].command({:serverStatus => 1})['opcounters']['query']
  end

  def assert_query_route(expected_target)
    #puts "#{test_connection.read_pool.port} #{expected_target.read_pool.port}"
    queries_before = query_count(expected_target)
    assert_nothing_raised do
      yield
    end
    queries_after = query_count(expected_target)
    assert_equal 1, queries_after - queries_before
  end
end
