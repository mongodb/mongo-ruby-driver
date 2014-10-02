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
    extend Forwardable

    # The default time in seconds to timeout a connection attempt.
    #
    # @since 2.0.0
    TIMEOUT = 5

    # @return [ Mongo::Auth::CR, Mongo::Auth::X509, Mongo::Auth:LDAP ]
    #   authenticator The authentication strategy.
    attr_reader :authenticator

    # @return [ Mongo::Server::Address ] address The address to connect to.
    attr_reader :address

    # @return [ Hash ] options The passed in options.
    attr_reader :options

    def_delegators :@server,
                   :write_command_enabled?,
                   :max_bson_object_size,
                   :max_message_size

    # Is this connection authenticated. Will return true if authorization
    # details were provided and authentication passed.
    #
    # @example Is the connection authenticated?
    #   connection.authenticated?
    #
    # @return [ true, false ] If the connection is authenticated.
    #
    # @since 2.0.0
    def authenticated?
      !!@authenticated
    end

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
        @socket = address.socket(timeout, ssl_options)
        @socket.connect!
        if authenticator # @todo: durran: and auth enabled?
          authenticator.login(self)
          @authenticated = true
        end
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
        @authenticated = false
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
      messages.last.replyable? ? read : nil
    end

    # Initialize a new socket connection from the client to the server.
    #
    # @example Create the connection.
    #   Connection.new(server)
    #
    # @param [ Mongo::Server ] server The server the connection is for.
    # @param [ Hash ] options The connection options.
    #
    # @since 2.0.0
    def initialize(server, options = {})
      @address = server.address
      @options = options.freeze
      @server = server
      @ssl_options = options.reject { |k, v| !k.to_s.start_with?('ssl') }
      @socket = nil
      setup_authentication!
    end

    # Get the connection timeout.
    #
    # @example Get the connection timeout.
    #   connection.timeout
    #
    # @return [ Float ] The connection timeout in seconds.
    #
    # @since 2.0.0
    def timeout
      @timeout ||= options[:socket_timeout] || TIMEOUT
    end

    private

    attr_reader :socket, :ssl_options

    def ensure_connected
      connect! if socket.nil? || !socket.alive?
      yield socket
    end

    def read
      ensure_connected{ |socket| Protocol::Reply.deserialize(socket) }
    end

    def setup_authentication!
      @authenticator = Auth.get(Auth::User.new(options)) if options[:user]
    end

    def write(messages, buffer = '')
      start_size = 0
      messages.each do |message|
        message.serialize(buffer, max_bson_object_size)
        if max_message_size &&
          (buffer.size - start_size) > max_message_size
          raise InvalidMessageSize.new(max_message_size)
          start_size = buffer.size
        end
      end
      ensure_connected{ |socket| socket.write(buffer) }
    end

    # Exception that is raised when trying to send a message that exceeds max
    # message size.
    #
    # @since 2.0.0
    class InvalidMessageSize < DriverError

      # The message is constant.
      #
      # @since 2.0.0
      MESSAGE = "Message exceeds allowed max message size."

      # Instantiate the new exception.
      #
      # @example Instantiate the exception.
      #   Mongo::Connection::InvalidMessageSize.new(max)
      #
      # @since 2.0.0
      def initialize(max_size = nil)
        super(max_size ?
                MESSAGE + " The max is #{max_size}." : MESSAGE)
      end
    end
  end
end
