# Copyright (C) 2013 10gen Inc.
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

class ShardingPoolManagerTest < Test::Unit::TestCase

  context "Initialization: " do

    setup do
      TCPSocket.stubs(:new).returns(new_mock_socket)
      @db = new_mock_db

      @client = stub("MongoShardedClient")
      @client.stubs(:connect_timeout).returns(5)
      @client.stubs(:op_timeout).returns(5)
      @client.stubs(:pool_size).returns(2)
      @client.stubs(:pool_timeout).returns(100)
      @client.stubs(:socket_class).returns(TCPSocket)
      @client.stubs(:mongos?).returns(true)
      @client.stubs(:[]).returns(@db)
      @client.stubs(:socket_opts)

      @client.stubs(:replica_set_name).returns(nil)
      @client.stubs(:log)
      @arbiters = ['localhost:27020']
      @hosts = [
        'localhost:27017',
        'localhost:27018',
        'localhost:27019'
      ]

      @ismaster = {
        'hosts' => @hosts,
        'arbiters' => @arbiters,
        'maxMessageSizeBytes' => 1024 * 2.5,
        'maxBsonObjectSize' => 1024
      }
    end

    should "populate pools correctly" do

      @db.stubs(:command).returns(
        # First call to get a socket.
        @ismaster.merge({'ismaster' => true}),

        # Subsequent calls to configure pools.
        @ismaster.merge({'ismaster' => true}),
        @ismaster.merge({'secondary' => true, 'maxMessageSizeBytes' => 700}),
        @ismaster.merge({'secondary' => true, 'maxBsonObjectSize' => 500}),
        @ismaster.merge({'arbiterOnly' => true})
      )

      seed = ['localhost:27017']
      manager = Mongo::ShardingPoolManager.new(@client, seed)
      @client.stubs(:local_manager).returns(manager)
      manager.connect

      formatted_seed = ['localhost', 27017]

      assert manager.seeds.include? formatted_seed
      assert_equal 500, manager.max_bson_size
      assert_equal 700 , manager.max_message_size
    end
  end
end
