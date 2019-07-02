# Copyright (C) 2014-2019 MongoDB, Inc.
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


      def start!
        @thread = Thread.new {
          while !@pool.closed? do
            @pool.populate
            @pool.populate_semaphore.wait(nil)
          end
        }
      end

      def stop!
        # Kill the thread instead of signaling so that if stop! is called during
        # populate or before the wait() on the semaphore, the thread still terminates
        @thread.kill if @thread
      end

      def running?
        @thread ? @thread.alive? : false
      end
    end
  end
end
