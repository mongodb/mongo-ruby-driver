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
include Mongo

class Cursor
  public :construct_query_spec
end

class ShardedClusterBasicTest < Test::Unit::TestCase

  def setup
    ensure_cluster(:sc)
    @document = { "name" => "test_user" }
    @seeds    = @sc.mongos_seeds
  end

  # TODO member.primary? ==> true
  def test_connect
    @client = sharded_connection
    assert @client.connected?
    assert_equal(@seeds.size, @client.seeds.size)
    probe(@seeds.size)
    @client.close
  end

  def test_connect_from_standard_client
    mongos = @seeds.first
    @client = MongoClient.new(*mongos.split(':'))
    assert @client.connected?
    assert @client.mongos?
    @client.close
  end

  def test_read_from_client
    host, port = @seeds.first.split(':')
    tags = [{:dc => "mongolia"}]
    @client = MongoClient.new(host, port, {:read => :secondary, :tag_sets => tags})
    assert @client.connected?
    cursor = Cursor.new(@client[TEST_DB]['whatever'], {})
    assert_equal cursor.construct_query_spec['$readPreference'], {:mode => 'secondary', :tags => tags}
  end

  def test_find_one_with_read_secondary
    @client = sharded_connection(:read => :secondary)
    @client[TEST_DB]["users"].insert([ @document ])
    assert_equal @client[TEST_DB]['users'].find_one["name"], "test_user"
  end

  def test_find_one_with_read_secondary_preferred
    @client = sharded_connection(:read => :secondary_preferred)
    @client[TEST_DB]["users"].insert([ @document ])
    assert_equal @client[TEST_DB]['users'].find_one["name"], "test_user"
  end

  def test_find_one_with_read_primary
    @client = sharded_connection(:read => :primary)
    @client[TEST_DB]["users"].insert([ @document ])
    assert_equal @client[TEST_DB]['users'].find_one["name"], "test_user"
  end

  def test_find_one_with_read_primary_preferred
    @client = sharded_connection(:read => :primary_preferred)
    @client[TEST_DB]["users"].insert([ @document ])
    assert_equal @client[TEST_DB]['users'].find_one["name"], "test_user"
  end

  def test_read_from_sharded_client
    tags = [{:dc => "mongolia"}]
    @client = sharded_connection(:read => :secondary, :tag_sets => tags)
    assert @client.connected?
    cursor = Cursor.new(@client[TEST_DB]['whatever'], {})
    assert_equal cursor.construct_query_spec['$readPreference'], {:mode => 'secondary', :tags => tags}
  end

  def test_hard_refresh
    @client = sharded_connection
    assert @client.connected?
    @client.hard_refresh!
    assert @client.connected?
    @client.close
  end

  def test_reconnect
    @client = sharded_connection
    assert @client.connected?
    router = @sc.servers(:routers).first
    router.stop
    probe(@seeds.size)
    assert @client.connected?
    @client.close
  end

  def test_mongos_failover
    @client = sharded_connection(:refresh_interval => 5, :refresh_mode => :sync)
    assert @client.connected?
    # do a find to pin a pool
    @client[TEST_DB]['test'].find_one
    original_primary = @client.manager.primary
    # stop the pinned member
    @sc.member_by_name("#{original_primary[0]}:#{original_primary[1]}").stop
    # assert that the client fails over to the next available mongos
    assert_nothing_raised do
      @client[TEST_DB]['test'].find_one
    end

    assert_not_equal original_primary, @client.manager.primary
    assert @client.connected?
    @client.close
  end

  def test_all_down
    @client = sharded_connection
    assert @client.connected?
    @sc.servers(:routers).each{|router| router.stop}
    assert_raises Mongo::ConnectionFailure do
      probe(@seeds.size)
    end
    assert_false @client.connected?
    @client.close
  end

  def test_cycle
    @client = sharded_connection
    assert @client.connected?
    routers = @sc.servers(:routers)
    while routers.size > 0 do
      rescue_connection_failure do
        probe(@seeds.size)
      end
      probe(@seeds.size)
      router = routers.detect{|r| r.port == @client.manager.primary.last}
      routers.delete(router)
      router.stop
    end
    assert_raises Mongo::ConnectionFailure do
      probe(@seeds.size)
    end
    assert_false @client.connected?
    routers = @sc.servers(:routers).reverse
    routers.each do |r|
      r.start
      @client.hard_refresh!
      rescue_connection_failure do
        probe(@seeds.size)
      end
      probe(@seeds.size)
    end
    @client.close
  end

  def test_wire_version_not_in_range
    [
      [Mongo::MongoClient::MAX_WIRE_VERSION+1, Mongo::MongoClient::MAX_WIRE_VERSION+1],
      [Mongo::MongoClient::MIN_WIRE_VERSION-1, Mongo::MongoClient::MIN_WIRE_VERSION-1]
    ].each do |min_wire_version_value, max_wire_version_value|
      Mongo.module_eval <<-EVAL
        class ShardingPoolManager
          def max_wire_version
            return #{max_wire_version_value}
          end
          def min_wire_version
            return #{min_wire_version_value}
          end
        end
      EVAL
      @client = MongoShardedClient.new(@seeds, :connect => false)
      assert !@client.connected?
      assert_raises Mongo::ConnectionFailure do
        @client.connect
      end
    end
    Mongo.module_eval <<-EVAL
      class ShardingPoolManager
        attr_reader :max_wire_version, :min_wire_version
      end
    EVAL
  end

  private

  def sharded_connection(opts={})
    client = MongoShardedClient.new(@seeds, opts)
    authenticate_client(client)
  end

  def probe(size)
    authenticate_client(@client)
    assert_equal(size, @client['config']['mongos'].find.to_a.size)
  end
end
