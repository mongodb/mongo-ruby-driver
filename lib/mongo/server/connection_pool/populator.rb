# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2019-2020 MongoDB Inc.
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
    class ConnectionPool
      # A manager that maintains the invariant that the
      # size of a connection pool is at least minPoolSize.
      #
      # @api private
      class Populator
        include BackgroundThread

        # @param [ Server::ConnectionPool ] pool The connection pool.
        # @param [ Hash ] options The options.
        #
        # @option options [ Logger ] :logger A custom logger to use.
        def initialize(pool, options = {})
          @pool = pool
          @thread = nil
          @options = options
        end

        attr_reader :options

        def pre_stop
          @pool.populate_semaphore.signal
        end

        private

        def do_work
          throw(:done) if @pool.closed?

          begin
            unless @pool.populate
              @pool.populate_semaphore.wait
            end
          rescue Error::AuthError, Error => e
            # Errors encountered when trying to add connections to
            # pool; try again later
            log_warn("Populator failed to connect a connection for #{@pool.address}: #{e.class}: #{e}.")
            @pool.populate_semaphore.wait(5)
          end
        end
      end
    end
  end
end
