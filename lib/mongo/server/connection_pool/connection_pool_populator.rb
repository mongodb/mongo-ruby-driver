# Copyright (C) 2019 MongoDB, Inc.
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
    # A manager that maintains the invariant that the
    # size of a connection pool is at least minPoolSize.
    #
    # @api private
    class ConnectionPoolPopulator
      def initialize(pool)
        @pool = pool
        @thread = nil
      end

      def run!
        if running?
          @thread
        else
          start!
        end
      end

      def stop!(wait = false)
        # Kill the thread instead of signaling so that if stop! is called during
        # populate or before the wait() on the semaphore, the thread still terminates
        if @thread
          @thread.kill
          if wait
            @thread.join
          end
          !@thread.alive?
        else
          true
        end
      end

      def running?
        if @thread
          @thread.alive?
        else
          false
        end
      end

      private

      def start!
        @thread = Thread.new do
          while !@pool.closed? do
            if @pool.populate
              @pool.populate_semaphore.wait
            else
              # Populate encountered connection errors; try again later
              @pool.populate_semaphore.wait(5)
            end
          end
        end
      end
    end
  end
end
