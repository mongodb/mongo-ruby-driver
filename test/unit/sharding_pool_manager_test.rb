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

    should "maintain seed format when checking connection health" do

      @db.stubs(:command).returns(
        # First call to get a socket.
        @ismaster.merge({'ismaster' => true}),

        # Subsequent calls to configure pools.
        @ismaster.merge({'ismaster' => true}),
        @ismaster.merge({'secondary' => true, 'maxMessageSizeBytes' => 700}),
        @ismaster.merge({'secondary' => true, 'maxBsonObjectSize' => 500}),
        @ismaster.merge({'arbiterOnly' => true})
      )

      config_db = new_mock_db
      mongos_coll = mock('collection')
      mongos_coll.stubs(:find).returns(@hosts.map{|h| {'_id' => h}})
      config_db.stubs(:[]).with('mongos').returns(mongos_coll)
      @client.stubs(:[]).with('config').returns(config_db)

      manager = Mongo::ShardingPoolManager.new(@client, @hosts)
      manager.check_connection_health

      assert manager.seeds.all? {|s| s.is_a?(Array) && s[0].is_a?(String) && s[1].is_a?(Integer)}
    end
  end
end
