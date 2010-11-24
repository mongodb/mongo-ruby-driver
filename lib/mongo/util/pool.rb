# encoding: UTF-8

# --
# Copyright (C) 2008-2010 10gen Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo
  class Pool

    attr_accessor :host, :port, :size, :timeout, :safe, :checked_out

    # Create a new pool of connections.
    #
    def initialize(connection, host, port, options={})
      @connection  = connection

      @host, @port = host, port

      # Pool size and timeout.
      @size      = options[:size] || 1
      @timeout   = options[:timeout]   || 5.0

      # Mutex for synchronizing pool access
      @connection_mutex = Mutex.new

      # Global safe option. This is false by default.
      @safe = options[:safe] || false

      # Create a mutex when a new key, in this case a socket,
      # is added to the hash.
      @safe_mutexes = Hash.new { |h, k| h[k] = Mutex.new }

      # Condition variable for signal and wait
      @queue = ConditionVariable.new

      @sockets      = []
      @checked_out  = []
    end

    def close
      @sockets.each do |sock|
        sock.close
      end
      @host = @port = nil
      @sockets.clear
      @checked_out.clear
    end

    # Return a socket to the pool.
    def checkin(socket)
      @connection_mutex.synchronize do
        @checked_out.delete(socket)
        @queue.signal
      end
      true
    end

    # Adds a new socket to the pool and checks it out.
    #
    # This method is called exclusively from #checkout;
    # therefore, it runs within a mutex.
    def checkout_new_socket
      begin
      socket = TCPSocket.new(@host, @port)
      socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
      rescue => ex
        raise ConnectionFailure, "Failed to connect socket: #{ex}"
      end
      @sockets << socket
      @checked_out << socket
      socket
    end

    # Checks out the first available socket from the pool.
    #
    # This method is called exclusively from #checkout;
    # therefore, it runs within a mutex.
    def checkout_existing_socket
      socket = (@sockets - @checked_out).first
      @checked_out << socket
      socket
    end

    # Check out an existing socket or create a new socket if the maximum
    # pool size has not been exceeded. Otherwise, wait for the next
    # available socket.
    def checkout
      @connection.connect if !@connection.connected?
      start_time = Time.now
      loop do
        if (Time.now - start_time) > @timeout
            raise ConnectionTimeoutError, "could not obtain connection within " +
              "#{@timeout} seconds. The max pool size is currently #{@size}; " +
              "consider increasing the pool size or timeout."
        end

        @connection_mutex.synchronize do
          socket = if @checked_out.size < @sockets.size
                     checkout_existing_socket
                   elsif @sockets.size < @size
                     checkout_new_socket
                   end

          return socket if socket

          # Otherwise, wait
          if @logger
            @logger.warn "MONGODB Waiting for available connection; " +
              "#{@checked_out.size} of #{@size} connections checked out."
          end
          @queue.wait(@connection_mutex)
        end
      end
    end
  end
end
