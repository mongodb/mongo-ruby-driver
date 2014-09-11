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
    @cluster.destroy
  end

  # Scenario: Replicated insert, update and delete timeout with W failure
  test 'Replicated insert, update and delete timeout with W failure' do
    # Given a basic replica set
    # When I insert a document with the write concern { “w”: <nodes + 1>, “timeout”: 1}
    # Then the insert fails write concern
    assert_raise Mongo::WriteConcernError do
      @coll.insert({'a' => 1}, :w => @n + 1, :wtimeout => 1)
    end
    # When I update a document with the write concern { “w”: <nodes + 1>, “timeout”: 1}
    # Then the update fails write concern
    assert_raise Mongo::WriteConcernError do
      @coll.update({'a' => 2}, {}, :w => @n + 1, :wtimeout => 1, :upsert => true)
    end
    # When I delete a document with the write concern { “w”: <nodes + 1>, “timeout”: 1}
    # Then the delete fails write concern
    @coll.insert({'a' => 3}, :w => @n)
    assert_raise Mongo::WriteConcernError do
      @coll.remove({'a' => 3}, :w => @n + 1, :wtimeout => 1)
    end
  end
end
