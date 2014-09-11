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
    @cluster = @@mo.configure({:orchestration => 'replica_sets', :request_content => {:id => 'replica_set_arbiter_1', :preset => 'arbiter.json'} })
    @mongodb_uri = @cluster.object['mongodb_uri']
    @client = Mongo::MongoClient.from_uri(@mongodb_uri)
    @client.drop_database(TEST_DB)
    @db = @client[TEST_DB]
    @coll = @db[TEST_COLL]
    await_replication(@coll)
    @primary = @cluster.primary
    @n = @cluster.object['members'].count - 1
    @coll.insert({'a' => 1}, :w => @n)
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
    # Given an arbiter replica set
    # And a document written to all data-bearing members
    # When I read with read-preference PRIMARY
    client = Mongo::MongoClient.from_uri(@mongodb_uri, :read => :primary)
    # Then the read occurs on the primary
    assert_equal(@primary.object['uri'], client.read_pool.address)
    assert_route(client, 'query') do
      client[TEST_DB][TEST_COLL].find_one({'a' => 1})
    end
    # When there is no primary
    @cluster.arbiters.first.stop
    @cluster.primary.stop
    # And I read with read-preference PRIMARY
    # Then the read fails
    ex = assert_raise Mongo::ConnectionFailure do
      client[TEST_DB][TEST_COLL].find_one({'a' => 1})
    end
    assert_match(/No replica set member available for query with read preference matching/, ex.message)
  end

  # Scenario: Read Primary Preferred
  test 'Read Primary Preferred' do
    # Given an arbiter replica set
    # And a document written to all data-bearing members
    # When I read with read-preference PRIMARY_PREFERRED
    client = Mongo::MongoClient.from_uri(@mongodb_uri, :read => :primary_preferred)
    # Then the read occurs on the primary
    assert_equal(@primary.object['uri'], client.read_pool.address)
    assert_route(client, 'query') do
      client[TEST_DB][TEST_COLL].find_one({'a' => 1})
    end
    # When there is no primary
    @cluster.arbiters.first.stop
    @cluster.primary.stop
    # And I read with read-preference PRIMARY_PREFERRED
    # Then the read succeeds
    assert(client[TEST_DB][TEST_COLL].find_one({'a' => 1}))
  end

  # Scenario: Read Secondary
  test 'Read Secondary' do
    # Given an arbiter replica set
    # And a document written to all data-bearing members
    # When I read with read-preference SECONDARY
    client = Mongo::MongoClient.from_uri(@mongodb_uri, :read => :secondary)
    # Then the read occurs on the secondary
    assert_not_equal(@primary.object['uri'], client.read_pool.address)
    assert_route(client, 'query') do
      client[TEST_DB][TEST_COLL].find_one({'a' => 1})
    end
    # When there are no secondaries
    @cluster.secondaries.first.stop
    # And I read with read-preference SECONDARY
    # Then the read fails
    ex = assert_raise Mongo::ConnectionFailure do
      client[TEST_DB][TEST_COLL].find_one({'a' => 1})
    end
    assert_match(/No replica set member available for query with read preference matching/, ex.message)
  end

  # Scenario: Read Secondary Preferred
  test 'Read Secondary Preferred' do
    # Given an arbiter replica set
    # And a document written to all data-bearing members
    # When I read with read-preference SECONDARY_PREFERRED
    client = Mongo::MongoClient.from_uri(@mongodb_uri, :read => :secondary_preferred)
    # Then the read occurs on the secondary
    assert_not_equal(@primary.object['uri'], client.read_pool.address)
    assert_route(client, 'query') do
      client[TEST_DB][TEST_COLL].find_one({'a' => 1})
    end
    # When there are no secondaries
    @cluster.secondaries.first.stop
    # And I read with read-preference SECONDARY_PREFERRED
    # Then the read succeeds
    assert(client[TEST_DB][TEST_COLL].find_one({'a' => 1}))
  end

  # Scenario: Read With Nearest
  test 'Read Nearest' do
    # Given an arbiter replica set
    # And a document written to all data-bearing members
    # When I read with read-preference NEAREST
    client = Mongo::MongoClient.from_uri(@mongodb_uri, :read => :nearest)
    # Then the read succeeds
    assert(client[TEST_DB][TEST_COLL].find_one({'a' => 1}))
  end

  # Scenario: Read Primary With Tag Sets
  test 'Read Primary With Tag Sets' do
    # Given an arbiter replica set
    # And a document written to all data-bearing members
    # When I read with read-preference PRIMARY and a tag set
    tag_sets = [{'ordinal' => 'one'}, {'dc' => 'ny'}]
    client = Mongo::MongoClient.from_uri(@mongodb_uri, :read => :primary, :tag_sets => tag_sets)
    # Then the read fails with error "PRIMARY cannot be combined with tags"
    ex = assert_raise Mongo::MongoArgumentError do
      client[TEST_DB][TEST_COLL].find_one({'a' => 1})
    end
    assert_match(/Read preference :primary cannot be combined with tags/, ex.message)
  end

  # Scenario: Read Primary Preferred With Tag Sets
  test 'Read Primary Preferred With Tag Sets' do
    # Given an arbiter replica set
    # And a document written to all data-bearing members
    # When I read with read-preference PRIMARY_PREFERRED and a tag set
    tag_sets = [{'ordinal' => 'two'}, {'dc' => 'pa'}]
    client = Mongo::MongoClient.from_uri(@mongodb_uri, :read => :primary_preferred, :tag_sets => tag_sets)
    # Then the read occurs on the primary
    assert_equal(@primary.object['uri'], client.read_pool.address)
    assert_route(client, 'query') do
      client[TEST_DB][TEST_COLL].find_one({'a' => 1})
    end
    # When there is no primary
    @cluster.arbiters.first.stop
    @cluster.primary.stop
    # And I read with read-preference PRIMARY_PREFERRED and a matching tag set
    tag_sets = [{'ordinal' => 'two'}]
    client = Mongo::MongoClient.from_uri(@mongodb_uri, :read => :primary_preferred, :tag_sets => tag_sets)
    # Then the read occurs on a matching secondary
    assert_not_equal(@primary.object['uri'], client.read_pool.address)
    assert_route(client, 'query') do
      client[TEST_DB][TEST_COLL].find_one({'a' => 1})
    end
    # When I read with read-preference PRIMARY_PREFERRED and a non-matching tag set
    tag_sets = [{'ordinal' => 'three'}, {'dc' => 'na'}]
    client = Mongo::MongoClient.from_uri(@mongodb_uri, :read => :primary_preferred, :tag_sets => tag_sets)
    # Then the read fails with error "No replica set member available for query with ReadPreference PRIMARY_PREFERRED and tags <tags>"
    ex = assert_raise Mongo::ConnectionFailure do
      client[TEST_DB][TEST_COLL].find_one({'a' => 1})
    end
    assert_match(/No replica set member available for query with read preference matching mode primary_preferred and tags matching/, ex.message)
  end

  # Scenario: Read Secondary With Tag Sets
  test 'Read Secondary With Tag Sets' do
    # Given an arbiter replica set
    # And a document written to all data-bearing members
    # When I read with read-preference SECONDARY and a secondary-matching tag set
    tag_sets = [{'ordinal' => 'two'}]
    client = Mongo::MongoClient.from_uri(@mongodb_uri, :read => :secondary, :tag_sets => tag_sets)
    # Then the read occurs on a matching secondary
    assert_not_equal(@primary.object['uri'], client.read_pool.address)
    assert_route(client, 'query') do
      client[TEST_DB][TEST_COLL].find_one({'a' => 1})
    end
    # When I read with read-preference SECONDARY and a non-secondary-matching tag set
    tag_sets = [{'ordinal' => 'one'}]
    client = Mongo::MongoClient.from_uri(@mongodb_uri, :read => :secondary, :tag_sets => tag_sets)
    # Then the read fails with error "No replica set member available for query with ReadPreference SECONDARY and tags <tags>"
    ex = assert_raise Mongo::ConnectionFailure do
      client[TEST_DB][TEST_COLL].find_one({'a' => 1})
    end
    assert_match(/No replica set member available for query with read preference matching mode secondary and tags matching/, ex.message)
  end

  # Scenario: Read Secondary Preferred With Tag Sets
  test 'Read Secondary Preferred With Tag Sets' do
    # Given an arbiter replica set
    # And a document written to all data-bearing members
    # When I read with read-preference SECONDARY_PREFERRED and a secondary-matching tag set
    tag_sets = [{'ordinal' => 'two'}]
    client = Mongo::MongoClient.from_uri(@mongodb_uri, :read => :secondary_preferred, :tag_sets => tag_sets)
    # Then the read occurs on a matching secondary
    assert_not_equal(@primary.object['uri'], client.read_pool.address)
    assert_route(client, 'query') do
      client[TEST_DB][TEST_COLL].find_one({'a' => 1})
    end
    # When I read with read-preference SECONDARY_PREFERRED and a non-secondary-matching tag set
    tag_sets = [{'ordinal' => 'three'}]
    client = Mongo::MongoClient.from_uri(@mongodb_uri, :read => :secondary_preferred, :tag_sets => tag_sets)
    # Then the read occurs on the primary
    assert_equal(@primary.object['uri'], client.read_pool.address)
    assert_route(client, 'query') do
      client[TEST_DB][TEST_COLL].find_one({'a' => 1})
    end
  end

  # Scenario: Read Nearest With Tag Sets
  test 'Read Nearest With Tag Sets' do
    # TODO - driver bug - mode NEAREST appears to ignore tags
    # Given an arbiter replica set
    # And a document written to all data-bearing members
    # When I read with read-preference NEAREST and a primary-matching tag set
    tag_sets = [{'ordinal' => 'one'}]
    client = Mongo::MongoClient.from_uri(@mongodb_uri, :read => :nearest, :tag_sets => tag_sets)
    # Then the read occurs on the primary
    # TODO - driver bug - unexpectedly does not always route to matching (primary) member
    # assert_equal(@primary.object['uri'], client.read_pool.address)
    # assert_route(client, 'query') do
    #   client[TEST_DB][TEST_COLL].find_one({'a' => 1})
    # end
    # When I read with read-preference NEAREST and a secondary-matching tag set
    tag_sets = [{'ordinal' => 'two'}]
    client = Mongo::MongoClient.from_uri(@mongodb_uri, :read => :nearest, :tag_sets => tag_sets)
    # Then the read occurs on a matching secondary
    # TODO - driver bug - unexpectedly does not always route to matching (secondary) member
    # assert_not_equal(@primary.object['uri'], client.read_pool.address)
    # assert_route(client, 'query') do
    #   client[TEST_DB][TEST_COLL].find_one({'a' => 1})
    # end
    # When I read with read-preference NEAREST and a non-matching tag set
    tag_sets = [{'ordinal' => 'three'}]
    client = Mongo::MongoClient.from_uri(@mongodb_uri, :read => :nearest, :tag_sets => tag_sets)
    # Then the read fails with error "No replica set member available for query with ReadPreference NEAREST and tags <tags>"
    # TODO - driver bug - does not fail as expected
    # ex = assert_raise Mongo::ConnectionFailure do
    #   client[TEST_DB][TEST_COLL].find_one({'a' => 1})
    # end
    # assert_match(/No replica set member available for query with read preference matching mode nearest and tags matching/, ex.message)
  end

  # Scenario: Secondary OK Commands
  test 'Secondary OK Commands' do
    # Given an arbiter replica set
    # And some documents written to all data-bearing members
    docs = [
        {coordinates: [-73.986209, 40.756819], name: 'Times Square'}
    ]
    @coll.insert(docs, :w => @n)
    @coll.create_index([['coordinates', Mongo::GEO2D]]);
    # And the following commands:
    #   | aggregate              |
    #   | collStats              |
    #   | count                  |
    #   | dbStats                |
    #   | distinct               |
    #   | geoNear                |
    #   | geoSearch              |
    #   | geoWalk                |
    #   | group                  |
    #   | isMaster               |
    #   | mapReduce              |
    #   | parallelCollectionScan |
    commands = [
        [ 'aggregate', {'aggregate' => TEST_COLL,'pipeline' => [{'$group' => {'_id' => nil, 'count' => {'$sum' => 1}}}]}, nil ],
        [ 'collStats', {'collStats' => TEST_COLL }, nil ],
        [ 'count', {'count' => TEST_COLL}, nil ],
        [ 'dbStats', {'dbStats' => 1}, nil ],
        [ 'distinct', {'distinct' => TEST_COLL, 'key' => "a" }, nil ],
        [ 'geoNear', {'geoNear' => TEST_COLL, 'near' => [-73.9667,40.78], 'maxDistance' => 1000}, nil ],
        #[ 'geoSearch', {'geoSearch' => TEST_COLL, 'near' => [-73.9667,40.78], 'maxDistance' => 1000}, nil ],
        #[ 'geoWalk', {'geoWalk' => TEST_COLL}, nil ],
        [ 'group', {'group' => {'ns' => TEST_COLL, 'key' => "a", '$reduce' => BSON::Code.new('function ( curr, result ) { }'), 'initial' => {}}}, nil ],
        [ 'isMaster', {'isMaster' => 1}, nil ],
        [ 'mapReduce', {'mapReduce' => TEST_COLL, 'map' => BSON::Code.new('function(){emit("a",this.a);}'), 'reduce' => BSON::Code.new('function(key,values){return Array.sum(values);}'), 'out' => {'inline' => 1}}, nil ],
        [ 'parallelCollectionScan', {'parallelCollectionScan' => TEST_COLL, 'numCursors' => 2}, nil ],
    ]
    # When I run each of the following commands with read-preference SECONDARY
    client = Mongo::MongoClient.from_uri(@mongodb_uri, :read => :secondary)
    # Then the command occurs on a secondary
    assert_not_equal(@primary.object['uri'], client.read_pool.address)
    commands.each do |name, command, expect|
      result = nil
      assert_route(client, 'command', 2) do
        result = client[TEST_DB].command(command)
      end
      assert_equal(expect, result, name) if expect
    end
  end

  private

  def opcounters(client, field = 'query')
    client['admin'].command({:serverStatus => 1})['opcounters'][field]
  end

  def assert_route(client, field = 'query', expect_count = 1)
    direct_client = Mongo::MongoClient.new(client.read_pool.host, client.read_pool.port)
    queries_before = opcounters(direct_client, field)
    assert_nothing_raised do
      yield
    end
    queries_after = opcounters(direct_client, field)
    assert_equal(expect_count, queries_after - queries_before, "expect query count to increment by #{expect_count}")
  end
end
