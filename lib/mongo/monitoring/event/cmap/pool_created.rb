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

        # Event published when a connection pool is created.
        #
        # @since 2.9.0
        class PoolCreated < Base

          # @return [ Mongo::Address ] address The address of the server the pool's connections will
          #   connect to.
          #
          # @since 2.9.0
          attr_reader :address

          # @return [ Hash ] options Options specified for pool creation.
          #
          # @since 2.9.0
          attr_reader :options

          # @return [ Mongo::Server::ConnectionPool ] pool The pool that was just
          # created.
          #
          # @since 2.11.0
          # @api experimental
          attr_reader :pool

          # Create the event.
          #
          # @example Create the event.
          #   PoolCreated.new(address, options, pool)
          #
          # @since 2.9.0
          # @api private
          def initialize(address, options, pool)
            @address = address
            @options = options.dup.freeze
            @pool = pool
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
                "address=#{address} options=#{options} pool=0x#{pool.object_id}>"
          end
        end
      end
    end
  end
end
