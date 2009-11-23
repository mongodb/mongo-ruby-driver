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
        @conn = Connection.new('localhost', 27107, :connect => false)

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

  context "Connection pooling: " do 
    setup do 
      TCPSocket.stubs(:new).returns(new_mock_socket)
      @conn = Connection.new('localhost', 27107, :connect => false, 
                                   :pool_size => 3)

      admin_db = new_mock_db
      admin_db.expects(:command).returns({'ok' => 1, 'ismaster' => 1})
      @conn.expects(:[]).with('admin').returns(admin_db)
      @conn.connect_to_master
    end

    should "check out a new connection" do 
      socket = @conn.checkout
      assert @conn.reserved_connections.keys.include? Thread.current.object_id
    end

    context "with multiple threads" do 
      setup do 
        @thread1 = Object.new
        @thread2 = Object.new
        @thread3 = Object.new
        @thread4 = Object.new
        
        Thread.stubs(:current).returns(@thread1)
        @socket1 = @conn.checkout
        Thread.stubs(:current).returns(@thread2)
        @socket2 = @conn.checkout
        Thread.stubs(:current).returns(@thread3)
        @socket3 = @conn.checkout
      end

      should "add each thread to the reserved pool" do 
        assert @conn.reserved_connections.keys.include?(@thread1.object_id)
        assert @conn.reserved_connections.keys.include?(@thread2.object_id)
        assert @conn.reserved_connections.keys.include?(@thread3.object_id)
      end

      should "only add one socket per thread" do 
        @conn.reserved_connections.values do |socket|
          assert [@socket1, @socket2, @socket3].include?(socket)
        end
      end

      should "check out all sockets" do 
        assert_equal @conn.sockets.size, @conn.checked_out.size
        @conn.sockets.each do |sock|
          assert @conn.checked_out.include?(sock)
        end
      end

      should "raise an error if no more sockets can be checked out" do
        # This can't be tested with mock threads.
        # Will test in integration tests.
      end

      context "when releasing dead threads" do 
        setup do 
          @thread1.expects(:alive?).returns(false)
          @thread2.expects(:alive?).returns(true)
          @thread3.expects(:alive?).returns(true)
          Thread.expects(:list).returns([@thread1, @thread2, @thread3])
          @conn.clear_stale_cached_connections!
        end

        should "return connections for dead threads" do 
          assert !@conn.checked_out.include?(@socket1)
          assert_nil @conn.reserved_connections[@thread1.object_id]
        end

        should "maintain connection for live threads" do 
          #assert @conn.checked_out.include?(@socket2)
          #assert @conn.checked_out.include?(@socket3)
        end
      end

      context "when checking in a socket" do 
        setup do 
          @conn.checkin(@socket3)
        end

        should "reduce the number checked out by one" do 
          #assert_equal @conn.checked_out.size, (@conn.sockets.size - 1)
        end
      end
    end
  end
end 

