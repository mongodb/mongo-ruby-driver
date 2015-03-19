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
        # @example Create the connection.
        #   Connection.new(address)
        #
        # @param [ Mongo::Address ] address The address the connection is for.
        # @param [ Hash ] options The connection options.
        #
        # @since 2.0.0
        def initialize(address, options = {})
          @address = address
          @options = options.freeze
          @ssl_options = options.reject { |k, v| !k.to_s.start_with?('ssl') }
          @socket = nil
          @pid = Process.pid
        end

        private

        def write(messages, buffer = '')
          messages.each{ |message| message.serialize(buffer) }
          ensure_connected{ |socket| socket.write(buffer) }
        end
      end
    end
  end
end
