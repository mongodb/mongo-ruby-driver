# Copyright (C) 2009-2013 MongoDB, Inc.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#  http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo
  module Pool

    # This class models the database connections and their behavior.
    class Connection

      DEFAULT_TIMEOUT = 5

      # @!attribute host
      #   @return [String] The hostname (or path for unix sockets).
      # @!attribute port
      #   @return [Integer] The port number (nil for unix sockets).
      # @!attribute timeout
      #   @return [Integer] The socket timeout value in seconds.
      # @!attribute last_use
      #   @return [Time.now] The most recent lease time for the connection.
      attr_reader :host, :port, :timeout, :last_use

      # Initializes a new connected and ready-to-use Connection instance.
      #
      # @example
      #   Connection.new('::1', 27015)
      #   Connection.new('localhost', 27015)
      #   Connection.new('/path/to/socket.sock', nil, 2)
      #   Connection.new('localhost', 27015, nil, { :ssl => true })
      #
      # @param host [String] The hostname (or path for unix sockets).
      # @param port [Integer] The port number (nil for unix sockets).
      # @param timeout [Integer] The socket timeout value in seconds.
      # @param opts [Hash] Optional settings and configuration values.
      #
      # @return [Connection] The connection instance.
      def initialize(host, port, timeout = nil, opts = {})
        @host     = host
        @port     = port
        @timeout  = timeout || DEFAULT_TIMEOUT
        @last_use = nil
        @socket   = nil
        @ssl_opts = opts.reject { |k, v| !k.to_s.start_with?('ssl') }
        connect if opts.fetch(:connect, true)
        self
      end

      # Leases the connection and sets the last use time to now.
      #
      # @example
      #   connection = Connection.new('::1', 27015)
      #   connection.lease
      #
      def lease
        @last_use = Time.now
      end

      # Expires the lease and sets last use to nil.
      #
      # @example
      #   connection = Connection.new('::1', 27015)
      #   connection.lease
      #   connection.expire
      #
      def expire
        @last_use = nil
      end

      # Indicates whether or not the lease has expired.
      #
      # @example
      #   connection = Connection.new('::1', 27015)
      #   connection.expired?
      #
      # @return [true, false] Boolean value indicating if the lease has
      def expired?
        @last_use.nil?
      end

      # Create a socket a connected socket instance.
      #
      # @example
      #   connection = Connection.new('::1', 27015, { :connect => false })
      #   connection.connect
      #
      def connect
        if @host && @port.nil?
          @socket = Socket::Unix.new(@host, @timeout)
        else
          if @ssl_opts && !@ssl_opts.empty?
            @socket = Socket::SSL.new(@host, @port, @timeout, @ssl_opts)
          else
            @socket = Socket::TCP.new(@host, @port, @timeout)
          end
        end
      end

      # Closes the socket and disposes of the socket instance.
      #
      # @example
      #   connection = Connection.new('::1', 27015)
      #   connection.disconnect
      #
      def disconnect
        if @socket
          @socket.close
          @socket = nil
        end
      end

      # TODO: read and write probably need to be dealt with in terms of
      # handling Operation and OperationResult but we're still trying to
      # figure out exactly what those look like.
      #
      # It seems like this should be something like:
      #
      #   @pool.with_connection do |conn|
      #     conn.read #=> OperationResult
      #     conn.write(operations) #=> Array<OperationResult>
      #   end
      #
      # For now, I'm just stubbing this out, but this is likely very
      # incomplete and it will need to change.

      # Reads data from the socket and returns the result as an array of
      # documents.
      #
      # @return [Array<Hash>] The documents from the reply.
      def read
        Protocol::Reply.deserialize(@socket).documents
      end

      # Serializes the message and writes the data to the connected socket.
      #
      # @return [Integer] The length in bytes of the data written.
      def write(message)
        @socket.write(message.serialize)
      end

    end

  end
end
