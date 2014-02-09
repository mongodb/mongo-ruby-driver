require 'mongo/pool/socket'
require 'mongo/pool/connection'

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

    def checkin(connection)
    end

    def checkout
    end

    def initialize(options = {}, &block)

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
