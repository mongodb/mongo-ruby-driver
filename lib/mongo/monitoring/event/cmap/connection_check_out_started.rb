# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2019-2020 MongoDB Inc.
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

        # Event published when a thread begins attempting to check a connection out of a pool.
        #
        # @since 2.9.0
        class ConnectionCheckOutStarted < Base

          # @return [ Mongo::Address ] address The address of the server that the connection will
          #   connect to.
          #
          # @since 2.9.0
          attr_reader :address

          # Create the event.
          #
          # @param [ Address ] address
          #
          # @since 2.9.0
          # @api private
          def initialize(address)
            @address = address
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
            "#<#{self.class.name.sub(/^Mongo::Monitoring::Event::Cmap::/, '')} address=#{address}>"
          end
        end
      end
    end
  end
end
