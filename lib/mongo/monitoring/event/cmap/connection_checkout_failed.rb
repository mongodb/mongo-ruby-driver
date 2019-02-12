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
      class Cmap

        # Event fired when a connection is unable to be checked out of a pool.
        #
        # @since 2.7.0
        class ConnectionCheckoutFailed < Mongo::Monitoring::Event::Cmap

          # @return [ String ] POOL_CLOSED Indicates that the connection checkout failed due to the
          #   pool already being closed.
          #
          # @since 2.7.0
          POOL_CLOSED = 'poolClosed'.freeze

          # @return [ String ] TIMEOUT Indicates that the connection checkout failed due to the
          #   timeout being reached before a connection became available.
          #
          # @since 2.7.0
          TIMEOUT = 'timeout'.freeze

          # @return [ Mongo::Address ] address The address of the server the connection would have
          #   connected to.
          #
          # @since 2.7.0
          attr_reader :address

          # @return [ String ] reason The reason a connection was unable to be acquired.
          #
          # @since 2.7.0
          attr_reader :reason

          # Create the event.
          #
          # @example Create the event.
          #   ConnectionCheckoutFailed.new(reason, address)
          #
          # @since 2.7.0
          def initialize(reason, address)
            @reason = reason
            @address = address
          end

          # Returns a concise yet useful summary of the event.
          #
          # @return [ String ] String summary of the event.
          #
          # @note This method is experimental and subject to change.
          #
          # @since 2.7.0
          # @api experimental
          def summary
            "#<#{self.class.name.sub(/^Mongo::Monitoring::Event::/, '')} address=#{address} " +
                "reason=#{reason}>"
          end
        end
      end
    end
  end
end
