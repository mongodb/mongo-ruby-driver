# Copyright (C) 2015 MongoDB, Inc.

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
  class Server
    class Monitor

      # This class models the monitor connections and their behavior.
      #
      # @since 2.0.0
      class Connection
        include Connectable

        # The command used for determining server status.
        #
        # @since 2.2.0
        ISMASTER = { :ismaster => 1 }.freeze

        # The constant for the ismaster command.
        #
        # @since 2.2.0
        ISMASTER_MESSAGE = Protocol::Query.new(Database::ADMIN, Database::COMMAND, ISMASTER, :limit => -1)

        # The raw bytes for the ismaster message.
        #
        # @since 2.2.0
        ISMASTER_BYTES = ISMASTER_MESSAGE.serialize.to_s.freeze

        # The default time in seconds to timeout a connection attempt.
        #
        # @since 2.1.2
        CONNECT_TIMEOUT = 10.freeze

        # Send the preserialized ismaster call.
        #
        # @example Send a preserialized ismaster message.
        #   connection.ismaster
        #
        # @return [ BSON::Document ] The ismaster result.
        #
        # @since 2.2.0
        def ismaster
          ensure_connected do |socket|
            socket.write(ISMASTER_BYTES)
            Protocol::Reply.deserialize(socket).documents[0]
          end
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
            socket.connect!
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

        # Initialize a new socket connection from the client to the server.
        #
        # @api private
        #
        # @example Create the connection.
        #   Connection.new(address)
        #
        # @note Connection must never be directly instantiated outside of a
        #   Monitor.
        #
        # @param [ Mongo::Address ] address The address the connection is for.
        # @param [ Hash ] options The connection options.
        #
        # @since 2.0.0
        def initialize(address, options = {})
          @address = address
          @options = options.freeze
          @ssl_options = options.reject { |k, v| !k.to_s.start_with?(SSL) }
          @socket = nil
          @pid = Process.pid
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
          @timeout ||= options[:connect_timeout] || CONNECT_TIMEOUT
        end
      end
    end
  end
end
