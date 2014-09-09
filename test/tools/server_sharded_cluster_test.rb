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

class ServerShardedClusterTest < Test::Unit::TestCase
  TEST_DB = name.underscore
  TEST_COLL = name.underscore

  @@mo = Mongo::Orchestration::Service.new

  def setup
    @cluster = @@mo.configure({:orchestration => 'sharded_clusters', :request_content => {:id => 'sharded_cluster_1', :preset => 'basic.json'} })
    @client = Mongo::MongoShardedClient.from_uri(@cluster.object['mongodb_uri'])
    @client.drop_database(TEST_DB)
    @db = @client[TEST_DB]
    @coll = @db[TEST_COLL]
    @routers = @cluster.routers
  end

  def teardown
    @client.drop_database(TEST_DB)
    @cluster.delete
  end

  # Scenario: mongos Router Failover - Failure and Recovery
  test 'mongos Router Failover - Failure and Recovery' do
    # Given a basic sharded cluster
    # When I insert a document
    @coll.insert({'a' => 1})
    # Then the insert succeeds
    assert(@coll.find_one({'a' => 1}))
    # When I stop router A
    @routers.first.stop
    # And I insert a document with retries
    rescue_connection_failure do
      @coll.insert({'a' => 2})
    end
    # Then the insert succeeds (eventually)
    assert(@coll.find_one({'a' => 2}))
    # When I stop router B
    @routers.last.stop
    # And I insert a document
    # Then the insert fails
    assert_raise Mongo::ConnectionFailure do
      @coll.insert({'a' => 3})
    end
    # When I start router B
    @routers.last.start
    # And I insert a document
    @coll.insert({'a' => 4})
    # Then the insert succeeds
    assert(@coll.find_one({'a' => 4}))
    # When I start router A
    @routers.first.start
    # And I insert a document
    @coll.insert({'a' => 5})
    # Then the insert succeeds
    assert(@coll.find_one({'a' => 5}))
    # When I stop router B
    @routers.last.stop
    # And I insert a document with retires
    rescue_connection_failure do
      @coll.insert({'a' => 6})
    end
    # Then the insert succeeds (eventually)
    assert(@coll.find_one({'a' => 6}))
  end

  # Scenario: mongos Router Restart
  test "mongos Router Restart" do
    # Given a basic sharded cluster
    # When I insert a document
    @coll.insert({'a' => 1})
    # Then the insert succeeds
    assert(@coll.find_one({'a' => 1}))
    # When I restart router A
    @routers.first.restart
    # And I insert a document with retries
    rescue_connection_failure do
      @coll.insert({'a' => 2})
    end
    # Then the insert succeeds (eventually)
    assert(@coll.find_one({'a' => 2}))
    # When I restart router B
    @routers.last.restart
    # And I insert a document with retries
    rescue_connection_failure do
      @coll.insert({'a' => 3})
    end
    # Then the insert succeeds (eventually)
    assert(@coll.find_one({'a' => 3}))
  end
end

