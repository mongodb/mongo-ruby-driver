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

    # This provides common behaviour for connection objects.
    #
    # @since 2.0.0
    module Connectable

      # The ssl option prefix.
      #
      # @since 2.1.0
      SSL = 'ssl'.freeze

      # The default time in seconds to timeout an operation executed on a socket.
      #
      # @since 2.0.0
      TIMEOUT = 5.freeze

      # @return [ Mongo::Address ] address The address to connect to.
      attr_reader :address

      # @return [ Hash ] options The passed in options.
      attr_reader :options

      # @return [ Integer ] pid The process id when the connection was created.
      attr_reader :pid

      # Determine if the server is connectable. This will check not only if the
      # connection exists, but if messages can send to it successfully.
      #
      # @example Is the server connectable?
      #   connection.connectable?
      #
      # @return [ true, false ] If the connection is connectable.
      #
      # @since 2.1.0
      def connectable?
        begin; ping; rescue; false; end
      end

      # Determine if the connection is currently connected.
      #
      # @example Is the connection connected?
      #   connection.connected?
      #
      # @return [ true, false ] If connected.
      #
      # @deprecated Use #connectable? instead
      def connected?
        !!@socket && @socket.alive?
      end

      # Get the timeout to execute an operation on a socket.
      #
      # @example Get the timeout to execute an operation on a socket.
      #   connection.timeout
      #
      # @return [ Float ] The operation timeout in seconds.
      #
      # @since 2.0.0
      def timeout
        @timeout ||= options[:socket_timeout] || TIMEOUT
      end

      private

      attr_reader :socket

      def ssl_options
        @ssl_options[:ssl] == true ? @ssl_options : {}
      end

      def ensure_connected
        ensure_same_process!
        connect!
        begin
          yield socket
        rescue Exception => e
          disconnect!
          raise e
        end
      end

      def ensure_same_process!
        if pid != Process.pid
          disconnect!
          @pid = Process.pid
        end
      end

      def read
        ensure_connected do |socket|
          Protocol::Reply.deserialize(socket, max_message_size)
        end
      end
    end
  end
end
