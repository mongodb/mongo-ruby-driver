# Copyright (C) 2019 MongoDB, Inc.
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

        # Event published when a connection is successfully checked out out of a pool.
        #
        # @since 2.9.0
        class ConnectionCheckedOut < Base

          # @return [ Mongo::Address ] address The address of the server that the connection will
          #   connect to.
          #
          # @since 2.9.0
          attr_reader :address

          # @return [ Integer ] connection_id The ID of the connection.
          #
          # @since 2.9.0
          attr_reader :connection_id

          # Create the event.
          #
          # @example Create the event.
          #   ConnectionCheckedOut.new(address, id)
          #
          # @since 2.9.0
          # @api private
          def initialize(address, id)
            @address = address
            @connection_id = id
          end

          # Returns a concise yet useful summary of the event.
          #
          # @return [ String ] String summary of the event.
          #
          # @note This method is experimental and subject to change.
          #
          # @since 2.9.0
          # @api experimental
          def summary
            "#<#{self.class.name.sub(/^Mongo::Monitoring::Event::Cmap::/, '')} " +
                "address=#{address} connection_id=#{connection_id}>"
          end
        end
      end
    end
  end
end
