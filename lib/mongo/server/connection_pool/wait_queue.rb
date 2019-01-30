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
    class ConnectionPool
      class WaitQueue
        extend Forwardable

        def initialize(available_queue)
          @available_queue = available_queue
          @wait_queue = []
          @mutex = Mutex.new
        end

        def clear!
          @mutex.synchronize do
            @wait_queue.each(&:broadcast)
            @wait_queue.clear
          end
        end

        def ready_for_next_thread
          @mutex.synchronize do
            @wait_queue.shift
            @wait_queue.first.broadcast unless @wait_queue.empty?
          end
        end

        def wait_until_front_of_queue(wait_timeout, deadline)
          semaphore = Semaphore.new

          @mutex.synchronize do
            @wait_queue << semaphore

            if @wait_queue.size == 1
              return
            end
          end

          semaphore.wait(wait_timeout)

          if deadline <= Time.now
            @mutex.synchronize { @wait_queue.delete(semaphore) }
            raise Error::WaitQueueTimeout.new(address, pool_size)
          end
        end

        def_delegators :available_queue, :address, :pool_size
      end
    end
  end
end
