# Copyright (C) 2016-2019  MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo
  class Monitoring
    module Event
      module Cmap

        # Event published when a connection is closed.
        #
        # @since 2.8.0
        class ConnectionClosed < Base

          # @return [ Symbol ] STALE Indicates that the connection was closed due to it being stale.
          #
          # @since 2.8.0
          STALE = :stale

          # @return [ Symbol ] IDLE Indicates that the connection was closed due to it being idle.
          #
          # @since 2.8.0
          IDLE = :idle

          # @return [ Symbol ] ERROR Indicates that the connection was closed due to it experiencing
          #   an error.
          #
          # @since 2.8.0
          ERROR = :error

          # @return [ Symbol ] POOL_CLOSED Indicates that the connection was closed due to the pool
          #   already being closed.
          #
          # @since 2.8.0
          POOL_CLOSED = :pool_closed

         # @return [ Symbol ] HANDSHAKE_FAILED Indicates that the connection was closed due to the
          #   connection handshake failing.
          #
          # @since 2.8.0
          HANDSHAKE_FAILED = :handshakeFailed

          # @return [ Symbol ] UNKNOWN Indicates that the connection was closed for an unknown reason.
          #
          # @since 2.8.0
          UNKNOWN = :unknown

          # @return [ Integer ] connection_id The ID of the connection.
          #
          # @since 2.8.0
          attr_reader :connection_id

          # @return [ Symbol ] reason The reason why the connection was closed.
          #
          # @since 2.8.0
          attr_reader :reason

          # @return [ Mongo::Address ] address The address of the server the pool's connections will
          #   connect to.
          #
          # @since 2.8.0
          attr_reader :address

          # Create the event.
          #
          # @example Create the event.
          #   ConnectionClosed.new(reason, address, id)
          #
          # @since 2.8.0
          def initialize(reason, address, id)
            @reason = reason
            @address = address
            @connection_id = id
          end

          # Returns a concise yet useful summary of the event.
          #
          # @return [ String ] String summary of the event.
          #
          # @note This method is experimental and subject to change.
          #
          # @since 2.8.0
          # @api experimental
          def summary
            "#<#{self.class.name.sub(/^Mongo::Monitoring::Event::Cmap::/, '')} " +
                "address=#{address} connection_id=#{connection_id} reason=#{reason}>"
          end
        end
      end
    end
  end
end
