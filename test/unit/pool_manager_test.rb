require 'test_helper'
include Mongo

class PoolManagerTest < Test::Unit::TestCase

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

      @client.stubs(:replica_set_name).returns(nil)
      @client.stubs(:log)
      @arbiters = ['localhost:27020']
      @hosts = ['localhost:27017', 'localhost:27018', 'localhost:27019',
        'localhost:27020']
    end

    should "populate pools correctly" do

      @db.stubs(:command).returns(
        # First call to get a socket.
        {'ismaster' => true, 'hosts' => @hosts, 'arbiters' => @arbiters},

        # Subsequent calls to configure pools.
        {'ismaster' => true, 'hosts' => @hosts, 'arbiters' => @arbiters},
        {'secondary' => true, 'hosts' => @hosts, 'arbiters' => @arbiters},
        {'secondary' => true, 'hosts' => @hosts, 'arbiters' => @arbiters},
        {'arbiterOnly' => true, 'hosts' => @hosts, 'arbiters' => @arbiters})

      seeds = [['localhost', 27017]]
      manager = Mongo::PoolManager.new(@client, seeds)
      manager.connect

      assert_equal ['localhost', 27017], manager.primary
      assert_equal 27017, manager.primary_pool.port
      assert_equal 2, manager.secondaries.length
      assert_equal 27018, manager.secondary_pools[0].port
      assert_equal 27019, manager.secondary_pools[1].port
      assert_equal [['localhost', 27020]], manager.arbiters
    end

    should "populate pools with single unqueryable seed" do

      @db.stubs(:command).returns(
        # First call to recovering node
        {'ismaster' => false, 'secondary' => false, 'hosts' => @hosts, 'arbiters' => @arbiters},

        # Subsequent calls to configure pools.
        {'ismaster' => false, 'secondary' => false, 'hosts' => @hosts, 'arbiters' => @arbiters},
        {'ismaster' => true, 'hosts' => @hosts, 'arbiters' => @arbiters},
        {'secondary' => true, 'hosts' => @hosts, 'arbiters' => @arbiters},
        {'arbiterOnly' => true, 'hosts' => @hosts, 'arbiters' => @arbiters})

      seeds = [['localhost', 27017]]
      manager = Mongo::PoolManager.new(@client, seeds)
      manager.connect

      assert_equal ['localhost', 27018], manager.primary
      assert_equal 27018, manager.primary_pool.port
      assert_equal 1, manager.secondaries.length
      assert_equal 27019, manager.secondary_pools[0].port
      assert_equal [['localhost', 27020]], manager.arbiters
    end

  end

end
