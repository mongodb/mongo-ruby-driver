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

class RelicaSetTest < Test::Unit::TestCase
  TEST_DB = 'replica_set_test'
  TEST_COLL = 'replica_set_test'

  @@mo = Mongo::Orchestration::Service.new

  def setup
    @cluster = @@mo.configure({:orchestration => 'rs', :request_content => {:id => 'replica_set_1', :preset => 'basic.json'} })
    @seed = 'mongodb://' + @cluster.object['uri']
    @client = Mongo::MongoReplicaSetClient.from_uri(@seed)
    @client.drop_database(TEST_DB)
    @db = @client[TEST_DB]
    @coll = @db[TEST_COLL]
    @admin = @client['admin']
    @retries = 60
  end

  def teardown
    @coll.remove({})
    @client.drop_database(TEST_DB)
    @cluster.delete
  end

  def reattempt(n = @retries)
    n.times do |i|
      begin
        yield
        break
      rescue Mongo::ConnectionFailure => ex
        assert_equal("Could not checkout a socket.", ex.message)
        print "#{i}?"
        sleep(1)
      end
    end
    puts
  end

  test 'Replica set primary stepdown via driver' do
    @coll.insert({'a' => 1})
    assert_equal([1], @coll.find({}, :sort => [['a', Mongo::ASCENDING]]).to_a.map{|doc| doc['a']})

    ex = assert_raise Mongo::OperationFailure do
      @admin.command({'replSetStepDown' => 60})
    end
    assert_equal("Database command 'replSetStepDown' failed: no secondaries within 10 seconds of my optime", ex.message)

    ex = assert_raise Mongo::ConnectionFailure do
      @admin.command({'replSetStepDown' => 60, 'force' => true})
    end
    assert_equal("Operation failed with the following exception: end of file reached", ex.message)

    reattempt do
      @coll.insert({'a' => 2})
    end
    assert_equal([1, 2], @coll.find({}, :sort => [['a', Mongo::ASCENDING]]).to_a.map{|doc| doc['a']})
  end

=begin
  test 'Replica set primary stepdown via mongo orchestration' do
    @coll.insert({'a' => 1})
    assert_equal([1], @coll.find({}, :sort => [['a', Mongo::ASCENDING]]).to_a.map{|doc| doc['a']})

    @primary_0 = @cluster.primary
    @primary_0_resource = @cluster.sub_resource(Mongo::Orchestration::Resource, 'primary')
    @primary_0_resource.put('stepdown')
    pp @primary_0_resource.message_summary unless @primary_0_resource.ok
    assert_true(@primary_0_resource.ok, 'expected primary stepdown')
    @primary_1 = @cluster.primary
    assert_not_equal(@primary_0.object['uri'], @primary_0.object['uri'], 'primary did not change')

    reattempt do
      @coll.insert({'a' => 2})
    end
    assert_equal([1, 2], @coll.find({}, :sort => [['a', Mongo::ASCENDING]]).to_a.map{|doc| doc['a']})
  end
=end
end
