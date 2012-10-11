require 'test_helper'
require 'thread'

class PoolTest < Test::Unit::TestCase
  include Mongo

  def setup
    @connection = standard_connection
  end

  def test_pool_affinity
    @pool = Pool.new(@connection, TEST_HOST, TEST_PORT, :size => 5)

    @threads    = []

    10.times do
      @threads << Thread.new do
        original_socket = @pool.checkout
        @pool.checkin(original_socket)
        5000.times do
          socket = @pool.checkout
          assert_equal original_socket, socket
          @pool.checkin(socket)
        end
      end
    end

    @threads.each { |t| t.join }
  end

  def test_pool_thread_pruning
    @pool = Pool.new(@connection, TEST_HOST, TEST_PORT, :size => 5)

    @threads = []

    10.times do
      @threads << Thread.new do
        50.times do
          socket = @pool.checkout
          @pool.checkin(socket)
        end
      end
    end

    @threads.each { |t| t.join }
    assert_equal 10, @pool.instance_variable_get(:@threads_to_sockets).size

    # Thread-socket pool
    10000.times do
      @pool.checkin(@pool.checkout)
    end

    assert_equal 1, @pool.instance_variable_get(:@threads_to_sockets).size
  end
end
