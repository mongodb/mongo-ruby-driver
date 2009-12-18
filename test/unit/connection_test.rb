require 'test/test_helper'

class ConnectionTest < Test::Unit::TestCase

  def new_mock_socket
    socket = Object.new
    socket.stubs(:setsockopt).with(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
    socket
  end

  def new_mock_db
    db = Object.new
  end

  context "Initialization: " do

    context "given a single node" do
      setup do
        TCPSocket.stubs(:new).returns(new_mock_socket)
        @conn = Connection.new('localhost', 27017, :connect => false)

        admin_db = new_mock_db
        admin_db.expects(:command).returns({'ok' => 1, 'ismaster' => 1})
        @conn.expects(:[]).with('admin').returns(admin_db)
        @conn.connect_to_master
      end

      should "set localhost and port to master" do
        assert_equal 'localhost', @conn.host
        assert_equal 27017, @conn.port
      end

      should "set connection pool to 1" do
        assert_equal 1, @conn.size
      end

      should "default slave_ok to false" do
        assert !@conn.slave_ok?
      end
    end
  end

  context "with a nonstandard port" do 
    setup do 
      TCPSocket.stubs(:new).returns(new_mock_socket)
      @conn = Connection.new('255.255.255.255', 2500, :connect => false)
      admin_db = new_mock_db
      admin_db.expects(:command).returns({'ok' => 1, 'ismaster' => 1})
      @conn.expects(:[]).with('admin').returns(admin_db)
      @conn.connect_to_master
    end

    should "set localhost and port correctly" do
      assert_equal '255.255.255.255', @conn.host
      assert_equal 2500, @conn.port
    end
  end
end

