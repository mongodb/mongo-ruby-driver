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

class WriteConcernTest < Test::Unit::TestCase
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
    @client.drop_database(TEST_DB)
    @cluster.delete
  end

  # Scenario: Replicated insert, update and delete timeout with W failure
  test 'Primary Step Down' do
    # Given a basic replica set
    # When I insert a document
    @coll.insert({'a' => 1})
    # Then the insert succeeds
    assert(@coll.find_one({'a' => 1}))
    # When I command the primary to step down
    assert(@primary.stepdown.ok)
    # And I insert a document with retries
    rescue_connection_failure do
      @coll.insert({'a' => 2})
    end
    # Then the insert succeeds (eventually)
    assert(@coll.find_one({'a' => 2}))
  end
end
