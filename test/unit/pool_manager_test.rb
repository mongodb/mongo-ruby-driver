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

class PoolManagerUnitTest < Test::Unit::TestCase

  context "Initialization: " do

    setup do
      TCPSocket.stubs(:new).returns(new_mock_socket)
      @db = new_mock_db

      @client = stub("MongoClient")
      @client.stubs(:connect_timeout).returns(5)
      @client.stubs(:op_timeout).returns(5)
      @client.stubs(:pool_size).returns(2)
      @client.stubs(:pool_timeout).returns(100)
      @client.stubs(:seeds).returns(['localhost:30000'])
      @client.stubs(:socket_class).returns(TCPSocket)
      @client.stubs(:mongos?).returns(false)
      @client.stubs(:[]).returns(@db)
      @client.stubs(:socket_opts)

      @client.stubs(:replica_set_name).returns(nil)
      @client.stubs(:log)
      @arbiters = ['localhost:27020']
      @hosts = [
        'localhost:27017',
        'localhost:27018',
        'localhost:27019',
        'localhost:27020'
      ]

      @ismaster = {
        'hosts' => @hosts,
        'arbiters' => @arbiters,
        'maxBsonObjectSize' => 1024,
        'maxMessageSizeBytes' => 1024 * 2.5,
        'maxWireVersion' => 1,
        'minWireVersion' => 0
      }
    end

    should "populate pools correctly" do

      @db.stubs(:command).returns(
        # First call to get a socket.
        @ismaster.merge({'ismaster' => true}),

        # Subsequent calls to configure pools.
        @ismaster.merge({'ismaster' => true}),
        @ismaster.merge({'secondary' => true, 'maxBsonObjectSize' => 500}),
        @ismaster.merge({'secondary' => true, 'maxMessageSizeBytes' => 700}),
        @ismaster.merge({'arbiterOnly' => true})
      )

      seeds = [['localhost', 27017]]
      manager = Mongo::PoolManager.new(@client, seeds)
      @client.stubs(:local_manager).returns(manager)
      manager.connect

      assert_equal ['localhost', 27017], manager.primary
      assert_equal 27017, manager.primary_pool.port
      assert_equal 2, manager.secondaries.length
      assert_equal [27018, 27019], manager.secondary_pools.map(&:port).sort
      assert_equal [['localhost', 27020]], manager.arbiters
      assert_equal 500, manager.max_bson_size
      assert_equal 700, manager.max_message_size
    end

    should "populate pools with single unqueryable seed" do

      @db.stubs(:command).returns(
        # First call to recovering node
        @ismaster.merge({'ismaster' => false, 'secondary' => false}),

        # Subsequent calls to configure pools.
        @ismaster.merge({'ismaster' => false, 'secondary' => false}),
        @ismaster.merge({'ismaster' => true}),
        @ismaster.merge({'secondary' => true}),
        @ismaster.merge({'arbiterOnly' => true})
      )

      seeds = [['localhost', 27017]]
      manager = PoolManager.new(@client, seeds)
      @client.stubs(:local_manager).returns(manager)
      manager.connect

      assert_equal ['localhost', 27018], manager.primary
      assert_equal 27018, manager.primary_pool.port
      assert_equal 1, manager.secondaries.length
      assert_equal 27019, manager.secondary_pools[0].port
      assert_equal [['localhost', 27020]], manager.arbiters
    end

    should "return clones of pool lists" do

      @db.stubs(:command).returns(
        # First call to get a socket.
        @ismaster.merge({'ismaster' => true}),

        # Subsequent calls to configure pools.
        @ismaster.merge({'ismaster' => true}),
        @ismaster.merge({'secondary' => true, 'maxBsonObjectSize' => 500}),
        @ismaster.merge({'secondary' => true, 'maxMessageSizeBytes' => 700}),
        @ismaster.merge({'arbiterOnly' => true})
      )

      seeds = [['localhost', 27017], ['localhost', 27018]]
      manager = Mongo::PoolManager.new(@client, seeds)
      @client.stubs(:local_manager).returns(manager)
      manager.connect

      assert_not_equal manager.instance_variable_get(:@arbiters).object_id, manager.arbiters.object_id
      assert_not_equal manager.instance_variable_get(:@secondaries).object_id, manager.secondaries.object_id
      assert_not_equal manager.instance_variable_get(:@secondary_pools).object_id, manager.secondary_pools.object_id
      assert_not_equal manager.instance_variable_get(:@hosts).object_id, manager.hosts.object_id
      assert_not_equal manager.instance_variable_get(:@pools).object_id, manager.pools.object_id

      assert_not_equal manager.instance_variable_get(:@arbiters).object_id, manager.state_snapshot[:arbiters].object_id
      assert_not_equal manager.instance_variable_get(:@secondaries).object_id, manager.state_snapshot[:secondaries].object_id
      assert_not_equal manager.instance_variable_get(:@secondary_pools).object_id, manager.state_snapshot[:secondary_pools].object_id
      assert_not_equal manager.instance_variable_get(:@hosts).object_id, manager.state_snapshot[:hosts].object_id
      assert_not_equal manager.instance_variable_get(:@pools).object_id, manager.state_snapshot[:pools].object_id
    end

  end

end
