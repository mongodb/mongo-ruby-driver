require 'mongo/pool/socket'
require 'mongo/pool/connection'
require 'mongo/pool/queue'

module Mongo

  class SocketError < StandardError; end
  class SocketTimeoutError < SocketError; end
  class ConnectionError < StandardError; end

  class Pool

    # Used for synchronization of pools access.
    MUTEX = Mutex.new

    # The default max size for the connection pool.
    POOL_SIZE = 5

    # The default timeout for getting connections from the queue.
    TIMEOUT = 0.5

    # @return [ Hash ] options The pool options.
    attr_reader :options

    # Check a connection back into the pool. Will pull the connection from a
    # thread local stack that should contain it after it was checked out.
    #
    # @example Checkin the thread's connection to the pool.
    #   pool.checkin
    #
    # @since 3.0.0
    def checkin
      connection = pinned_connections.pop
      queue.enqueue(connection) if pinned_connections.empty?
    end

    # Check a connection out from the pool.
    #
    # @example Check a connection out from the pool.
    #   pool.checkout
    #
    # @return [ Mongo::Pool::Connection ] The checked out connection.
    #
    # @since 3.0.0
    def checkout
      if pinned_connections.empty?
        connection = queue.dequeue(timeout)
      else
        connection = pinned_connections.pop
      end
      stack.push(connection) and connection
    end

    # Create the new connection pool.
    #
    # @example Create the new connection pool.
    #   Pool.new(timeout: 0.5) do
    #     Connection.new
    #   end
    #
    # @note A block must be passed to set up the connections on initialization.
    #
    # @param [ Hash ] options The connection pool options.
    #
    # @since 3.0.0
    def initialize(options = {}, &block)
      @options = options
      @queue = Queue.new(pool_size, &block)
      @identifier = :"mongodb-pool-#{queue.object_id}"
    end

    # Get the default size of the connection pool.
    #
    # @example Get the pool size.
    #   pool.pool_size
    #
    # @return [ Integer ] The size of the pool.
    #
    # @since 3.0.0
    def pool_size
      @pool_size ||= options[:pool_size] || POOL_SIZE
    end

    # Get the timeout for checking connections out from the pool.
    #
    # @example Get the pool timeout.
    #   pool.timeout
    #
    # @return [ Float ] The pool timeout.
    #
    # @since 3.0.0
    def timeout
      @timeout ||= options[:timeout] || TIMEOUT
    end

    private

    attr_reader :queue, :identifier

    def pinned_connections
      ::Thread.current[identifier] ||= []
    end

    class << self

      # Get a connection pool for the provided server.
      #
      # @example Get a connection pool.
      #   Mongo::Pool.get(server)
      #
      # @param [ Mongo::Server ] server The server.
      #
      # @return [ Mongo::Pool ] The connection pool.
      #
      # @since 3.0.0
      def get(server)
        MUTEX.synchronize do
          pools[server.address] ||= create_pool(server)
        end
      end

      private

      def create_pool(server)
        Pool.new(
          size: server.options[:pool_size] || POOL_SIZE,
          timeout: server.options[:pool_timeout] || TIMEOUT
        ) do
          Connection.new(
            server.address.ip,
            server.address.port,
            server.options[:timeout] || Connection::TIMEOUT,
            server.options
          )
        end
      end

      def pools
        @pools ||= {}
      end
    end
  end
end
