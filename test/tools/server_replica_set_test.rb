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

class ServerRelicaSetTest < Test::Unit::TestCase
  TEST_DB = name.underscore
  TEST_COLL = name.underscore

  @@mo = Mongo::Orchestration::Service.new

  def setup
    @cluster = @@mo.configure({:orchestration => 'replica_sets', :request_content => {:id => 'replica_set_1', :preset => 'basic.json'} })
    @client = Mongo::MongoClient.from_uri(@cluster.object['mongodb_uri'])
    @client.drop_database(TEST_DB)
    @db = @client[TEST_DB]
    @coll = @db[TEST_COLL]
    await_replication(@coll)
    @primary = @cluster.primary
    @admin = @client['admin']
  end

  def teardown
    @client.drop_database(TEST_DB)
    @cluster.delete
  end

  def await_replication(coll)
    coll.insert({'a' => 0}, :w => 3)
  end

  def primary_stepdown
    if true
      assert(@primary.stepdown.ok)
    else
      ex = assert_raise Mongo::ConnectionFailure do
        @admin.command({'replSetStepDown' => 60, 'force' => true})
      end
      assert_equal("Operation failed with the following exception: end of file reached", ex.message)
    end
  end

  # Scenario: Primary Step Down
  test 'Primary Step Down' do
    # Given a basic replica set
    # And a client connected to it
    # When I insert a document
    @coll.insert({'a' => 1})
    # Then the insert succeeds
    assert(@coll.find_one({'a' => 1}))
    # When I command the primary to step down
    primary_stepdown
    # And I insert a document with retries
    rescue_connection_failure do
      @coll.insert({'a' => 2})
    end
    # Then the insert succeeds (eventually)
    assert(@coll.find_one({'a' => 2}))
  end
end
