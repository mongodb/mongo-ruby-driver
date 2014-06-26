# Copyright (C) 2009-2014 MongoDB, Inc.

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

  # This class models the socket connections and their behavior.
  #
  # @since 2.0.0
  class Connection

    # The default time in seconds to timeout a connection attempt.
    #
    # @since 2.0.0
    TIMEOUT = 5

    # @return [ Mongo::Server::Address ] address The address to connect to.
    attr_reader :address

    # @return [ Float ] timeout The connection timeout.
    attr_reader :timeout

    # Tell the underlying socket to establish a connection to the host.
    #
    # @example Connect to the host.
    #   connection.connect!
    #
    # @note This method mutates the connection class by setting a socket if
    #   one previously did not exist.
    #
    # @return [ true ] If the connection succeeded.
    #
    # @since 2.0.0
    def connect!
      unless socket
        @socket = address.socket(timeout, ssl_opts)
        @socket.connect!
      end
      true
    end

    # Disconnect the connection.
    #
    # @example Disconnect from the host.
    #   connection.disconnect!
    #
    # @note This method mutates the connection by setting the socket to nil
    #   if the closing succeeded.
    #
    # @return [ true ] If the disconnect succeeded.
    #
    # @since 2.0.0
    def disconnect!
      if socket
        socket.close
        @socket = nil
      end
      true
    end

    # Dispatch the provided messages to the connection. If the last message
    # requires a response a reply will be returned.
    #
    # @example Dispatch the messages.
    #   connection.dispatch([ insert, command ])
    #
    # @note This method is named dispatch since 'send' is a core Ruby method on
    #   all objects.
    #
    # @param [ Array<Message> ] messages The messages to dispatch.
    #
    # @return [ Protocol::Reply ] The reply if needed.
    #
    # @since 2.0.0
    def dispatch(messages)
      write(messages)
      messages.last.replyable? ? read : self
    end

    # Initialize a new socket connection from the client to the server.
    #
    # @example Create the connection.
    #   Connection.new(address, 10)
    #
    # @param [ Mongo::Server::Address ] address The address to connect to.
    # @param [ Float ] timeout The connection timeout.
    # @param [ Hash ] options The connection options.
    #
    # @since 2.0.0
    def initialize(address, timeout = nil, options = {})
      @address  = address
      @timeout  = timeout || TIMEOUT
      @ssl_opts = options.reject { |k, v| !k.to_s.start_with?('ssl') }
      @socket   = nil
    end

    # Read a reply from the connection.
    #
    # @example Read a reply from the connection.
    #   connection.read
    #
    # @return [ Protocol::Reply ] The reply object.
    #
    # @since 2.0.0
    def read
      ensure_connected do |socket|
        Protocol::Reply.deserialize(socket)
      end
    end

    # Write messages to the connection in a single network call.
    #
    # @example Write the messages to the connection.
    #   connection.write([ insert ])
    #
    # @note All messages must be instances of Mongo::Protocol::Message.
    #
    # @param [ Array<Message> ] messages The messages to write.
    # @param [ String ] buffer The buffer to write to.
    #
    # @return [ Connection ] The connection itself.
    #
    # @since 2.0.0
    def write(messages, buffer = '')
      messages.each do |message|
        message.serialize(buffer)
      end
      ensure_connected do |socket|
        socket.write(buffer)
      end
    end

    private

    attr_reader :socket, :ssl_opts

    def ensure_connected
      connect! if socket.nil? || !socket.alive?
      yield socket
    end
  end
end
