# Copyright (C) 2015-2019 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo
  class Server

    # This provides common behavior for connection objects.
    #
    # @since 2.0.0
    module Connectable

      # The ssl option prefix.
      #
      # @since 2.1.0
      # @deprecated
      SSL = 'ssl'.freeze

      # The default time in seconds to timeout an operation executed on a socket.
      #
      # @since 2.0.0
      #
      # @deprecated Timeouts on Ruby sockets aren't effective so this default
      #   option is no longer used. Will be removed in driver version 3.0.
      TIMEOUT = 5.freeze

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
      #
      # @deprecated No longer necessary with Server Selection specification.
      def connectable?; end

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

      protected

      attr_reader :socket

      private

      def ssl_options
        @ssl_options ||= if options[:ssl]
          options.select { |k, v| k.to_s.start_with?('ssl') }
        else
          {}
        end.freeze
      end

      def ensure_connected
        ensure_same_process!
        begin
          connect!
          result = yield socket
          success = true
          result
        ensure
          unless success
            disconnect!(reason: :error)
          end
        end
      end

      def ensure_same_process!
        if pid != Process.pid
          # When we reconnect here, CMAP events won't be correctly sent
          # since the CMAP spec does not permit a connection to be disconnected
          # and then reconnected
          log_warn("Detected PID change - Mongo client should have been reconnected (old pid #{pid}, new pid #{Process.pid}")
          disconnect!(reason: :stale)
          @closed = false
          @pid = Process.pid
          connect!
        end
      end
    end
  end
end
