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

class ServerStandaloneTest < Test::Unit::TestCase
  TEST_DB = name.underscore
  TEST_COLL = name.underscore

  @@mo = Mongo::Orchestration::Service.new

  def setup
    @server = @@mo.configure({:orchestration => 'servers', :request_content => {:id => 'standalone', :preset => 'basic.json'} })
    @client = Mongo::MongoClient.from_uri(@server.object['mongodb_uri'])
    @client.drop_database(TEST_DB)
    @db = @client[TEST_DB]
    @coll = @db[TEST_COLL]
  end

  def teardown
    @client.drop_database(TEST_DB)
    @server.delete
  end

  # Scenario: Server Failure and Recovery
  test 'Server Failure and Recovery' do
    # Given a basic standalone server
    # When I insert a document
    @coll.insert({'a' => 1})
    # Then the insert succeeds
    assert(@coll.find_one({'a' => 1}))
    # When I stop the server
    @server.stop
    # And I insert a document
    # Then the insert fails
    assert_raise Mongo::ConnectionFailure do
      @coll.insert({'a' => 2})
    end
    # When I start the server
    @server.start
    # And I insert a document
    @coll.insert({'a' => 3})
    # Then the insert succeeds
    assert(@coll.find_one({'a' => 3}))
  end

  # Scenario: Server Restart
  test 'Server Restart' do
    # Given a basic standalone server
    # When I insert a document
    @coll.insert({'a' => 1})
    # Then the insert succeeds
    assert(@coll.find_one({'a' => 1}))
    # When I restart the server
    @server.restart
    # And I insert a document with retries
    rescue_connection_failure do
      @coll.insert({'a' => 2})
    end
    # Then the insert succeeds (eventually)
    assert(@coll.find_one({'a' => 2}))
  end
end
