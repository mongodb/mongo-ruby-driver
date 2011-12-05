require './test/test_helper'
include Mongo

class PoolManagerTest < Test::Unit::TestCase

  context "Initialization: " do

    should "populate pools correctly" do
      TCPSocket.stubs(:new).returns(new_mock_socket)
      @db = new_mock_db

      @connection = stub("Connection")
      @connection.stubs(:connect_timeout).returns(5000)
      @connection.stubs(:pool_size).returns(2)
      @connection.stubs(:pool_timeout).returns(100)
      @connection.stubs(:seeds).returns(['localhost:30000'])
      @connection.stubs(:socket_class).returns(TCPSocket)
      @connection.stubs(:[]).returns(@db)

      @connection.stubs(:replica_set_name).returns(nil)
      @connection.stubs(:log)
      @arbiters = ['localhost:27020']
      @hosts = ['localhost:27017', 'localhost:27018', 'localhost:27019',
        'localhost:27020']

      @db.stubs(:command).returns(
        # First call to get a socket.
        {'ismaster' => true, 'hosts' => @hosts, 'arbiters' => @arbiters},

        # Subsequent calls to configure pools.
        {'ismaster' => true, 'hosts' => @hosts, 'arbiters' => @arbiters},
        {'secondary' => true, 'hosts' => @hosts, 'arbiters' => @arbiters},
        {'secondary' => true, 'hosts' => @hosts, 'arbiters' => @arbiters},
        {'arbiterOnly' => true, 'hosts' => @hosts, 'arbiters' => @arbiters})

      seeds = [['localhost', 27017]]
      manager = Mongo::PoolManager.new(@connection, seeds)
      manager.connect

      assert_equal ['localhost', 27017], manager.primary
      assert_equal 27017, manager.primary_pool.port
      assert_equal 2, manager.secondaries.length
      assert_equal 27018, manager.secondary_pools[0].port
      assert_equal [['localhost', 27020]], manager.arbiters
    end

  end

end
